use super::*;

#[tokio::test]
async fn test_s3_put_write_etag_preconditions() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-write-preconditions").await;

    let client = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("s3-write-preconditions");
    let key = "preconditioned.txt";

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created once"))
        .send()
        .await
        .expect("If-None-Match create should succeed when object is absent");
    let duplicate_create = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created twice"))
        .send()
        .await;
    assert!(
        duplicate_create.is_err(),
        "If-None-Match create should reject existing object"
    );

    let head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("HEAD should return current ETag");
    let etag = head.e_tag().expect("current ETag").to_string();

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .body(ByteStream::from_static(b"updated through If-Match"))
        .send()
        .await
        .expect("matching If-Match PUT should update the object");
    let stale_update = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .body(ByteStream::from_static(b"stale update"))
        .send()
        .await;
    assert!(
        stale_update.is_err(),
        "stale If-Match PUT should reject the update"
    );

    let updated_head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("HEAD should return updated ETag");
    let updated_etag = updated_head.e_tag().expect("updated ETag").to_string();
    client
        .copy_object()
        .bucket(&bucket)
        .key("copied-through-source-if-match.txt")
        .copy_source(format!("{bucket}/{key}"))
        .copy_source_if_match(&updated_etag)
        .send()
        .await
        .expect("matching source If-Match CopyObject should succeed");
    let stale_copy = client
        .copy_object()
        .bucket(&bucket)
        .key("stale-copy.txt")
        .copy_source(format!("{bucket}/{key}"))
        .copy_source_if_match(&etag)
        .send()
        .await;
    assert!(
        stale_copy.is_err(),
        "stale source If-Match CopyObject should fail"
    );
    let matching_none_match_copy = client
        .copy_object()
        .bucket(&bucket)
        .key("none-match-copy.txt")
        .copy_source(format!("{bucket}/{key}"))
        .copy_source_if_none_match(&updated_etag)
        .send()
        .await;
    assert!(
        matching_none_match_copy.is_err(),
        "matching source If-None-Match CopyObject should fail"
    );
}

#[tokio::test]
async fn test_s3_list_versions_and_get_filter_by_relationship_authorization() {
    let cluster = shared_docker_test_cluster().await;

    let writer_app = unique_test_name("s3-relationship-writer");
    let writer = create_docker_storage_test_actor(&cluster, &writer_app).await;

    let reader_app = unique_test_name("s3-relationship-reader");
    let (_reader_app_id, reader_client_id, reader_client_secret) = cluster
        .create_application_with_id(writer.tenant_id, &reader_app)
        .await;

    let bucket = unique_test_name("s3-relationship-filter");

    let writer_s3 = s3_client_for_docker_app(&cluster, &writer);
    let reader_credentials = aws_sdk_s3::config::Credentials::new(
        &reader_client_id,
        &reader_client_secret,
        None,
        None,
        "static",
    );
    let reader_config = aws_sdk_s3::Config::builder()
        .credentials_provider(reader_credentials)
        .region(aws_sdk_s3::config::Region::new(cluster.region.clone()))
        .endpoint_url(&writer.grpc_addr)
        .force_path_style(true)
        .behavior_version(aws_config::BehaviorVersion::latest())
        .build();
    let reader = Client::from_conf(reader_config);

    let allowed_key = "docs/allowed.txt";
    let denied_key = "docs/denied.txt";
    let visible_nested_key = "visible/nested.txt";
    let hidden_nested_key = "hidden/nested.txt";

    writer_s3
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("writer should create private bucket");
    cluster
        .grant_application_policy(writer.tenant_id, &reader_app, "object:list", &bucket)
        .await;
    writer_s3
        .put_object()
        .bucket(&bucket)
        .key(allowed_key)
        .body(ByteStream::from_static(b"allowed-v1"))
        .send()
        .await
        .expect("writer should put allowed v1");
    writer_s3
        .put_object()
        .bucket(&bucket)
        .key(denied_key)
        .body(ByteStream::from_static(b"denied"))
        .send()
        .await
        .expect("writer should put denied object");
    writer_s3
        .put_object()
        .bucket(&bucket)
        .key(allowed_key)
        .body(ByteStream::from_static(b"allowed-v2"))
        .send()
        .await
        .expect("writer should put allowed v2");
    writer_s3
        .put_object()
        .bucket(&bucket)
        .key(visible_nested_key)
        .body(ByteStream::from_static(b"visible"))
        .send()
        .await
        .expect("writer should put visible nested object");
    writer_s3
        .put_object()
        .bucket(&bucket)
        .key(hidden_nested_key)
        .body(ByteStream::from_static(b"hidden"))
        .send()
        .await
        .expect("writer should put hidden nested object");

    let ungranted = reader
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("reader has bucket list permission");
    assert!(
        ungranted.contents().is_empty(),
        "list permission alone must not reveal object keys"
    );

    for key in [allowed_key, "docs/not-created.txt"] {
        let head_denied = reader.head_object().bucket(&bucket).key(key).send().await;
        let rendered = format!("{head_denied:?}");
        assert!(
            head_denied.is_err()
                && (rendered.contains("403")
                    || rendered.contains("Forbidden")
                    || rendered.contains("Permission denied")),
            "S3 HEAD without object read permission must be denied for {key}: {rendered}"
        );
        assert!(
            !rendered.contains("NotFound"),
            "S3 HEAD without object read permission must not reveal absence for {key}: {rendered}"
        );
    }

    let mut auth_client = AuthServiceClient::connect(writer.grpc_addr.clone())
        .await
        .unwrap();
    for key in [allowed_key, visible_nested_key] {
        let mut grant = Request::new(GrantAccessRequest {
            grantee_app_id: reader_app.clone(),
            resource: format!("{bucket}/{key}"),
            action: "object:read".to_string(),
        });
        grant.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", writer.token).parse().unwrap(),
        );
        auth_client.grant_access(grant).await.unwrap();
    }

    let docs = reader
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("relationship-filtered docs list should succeed");
    assert_eq!(docs.contents().len(), 1);
    assert_eq!(docs.contents()[0].key(), Some(allowed_key));

    let tree = reader
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await
        .expect("relationship-filtered delimiter list should succeed");
    let prefixes = tree
        .common_prefixes()
        .iter()
        .filter_map(|prefix| prefix.prefix())
        .collect::<Vec<_>>();
    assert_eq!(prefixes, vec!["docs/", "visible/"]);

    let versions = reader
        .list_object_versions()
        .bucket(&bucket)
        .send()
        .await
        .expect("relationship-filtered version list should succeed");
    let version_keys = versions
        .versions()
        .iter()
        .filter_map(|version| version.key())
        .collect::<Vec<_>>();
    assert_eq!(
        version_keys,
        vec![allowed_key, allowed_key, visible_nested_key]
    );

    let allowed = reader
        .get_object()
        .bucket(&bucket)
        .key(allowed_key)
        .send()
        .await
        .expect("relationship grant should allow S3 GET");
    let allowed_bytes = allowed.body.collect().await.unwrap().into_bytes();
    assert_eq!(allowed_bytes.as_ref(), b"allowed-v2");

    reader
        .head_object()
        .bucket(&bucket)
        .key(allowed_key)
        .send()
        .await
        .expect("relationship grant should allow S3 HEAD");

    let denied = reader
        .get_object()
        .bucket(&bucket)
        .key(denied_key)
        .send()
        .await;
    assert!(
        denied.is_err(),
        "S3 GET must not allow ungranted object through bucket list permission"
    );
}

// Internal-only: directly compacts object metadata through cluster.states persistence.
#[tokio::test]
async fn test_s3_reads_and_lists_survive_object_metadata_compaction() {
    let cluster = shared_default_test_cluster().await;

    let app_name = unique_test_name("s3-compact");
    let (client_id, client_secret) = create_app(&cluster, &app_name).await;
    grant_storage_tenant_owner_for_test(&cluster, &app_name).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = unique_test_name("s3-compact");

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    client
        .put_object()
        .bucket(&bucket)
        .key("logs/a.txt")
        .body(ByteStream::from_static(b"a-v1"))
        .send()
        .await
        .expect("put logs/a.txt v1 should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("logs/a.txt")
        .body(ByteStream::from_static(b"a-v2"))
        .send()
        .await
        .expect("put logs/a.txt v2 should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("logs/b.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .expect("put logs/b.txt should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("logs/nested/c.txt")
        .body(ByteStream::from_static(b"c"))
        .send()
        .await
        .expect("put logs/nested/c.txt should succeed");
    client
        .delete_object()
        .bucket(&bucket)
        .key("logs/b.txt")
        .send()
        .await
        .expect("delete logs/b.txt should create a delete marker");

    let bucket_record = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    cluster.states[0]
        .persistence
        .compact_object_metadata(bucket_record.id)
        .await
        .unwrap()
        .expect("object metadata compaction should seal a manifest");

    let get_a = client
        .get_object()
        .bucket(&bucket)
        .key("logs/a.txt")
        .send()
        .await
        .expect("GET after compaction should succeed");
    let bytes = get_a
        .body
        .collect()
        .await
        .expect("collect compacted object body")
        .into_bytes();
    assert_eq!(bytes.as_ref(), b"a-v2");

    let compacted_listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("logs/")
        .delimiter("/")
        .send()
        .await
        .expect("delimiter LIST after compaction should succeed");
    assert_eq!(compacted_listing.contents().len(), 1);
    assert_eq!(compacted_listing.contents()[0].key(), Some("logs/a.txt"));
    assert_eq!(compacted_listing.common_prefixes().len(), 1);
    assert_eq!(
        compacted_listing.common_prefixes()[0].prefix(),
        Some("logs/nested/")
    );

    let compacted_versions = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("logs/a.txt")
        .send()
        .await
        .expect("version LIST after compaction should succeed");
    assert_eq!(compacted_versions.versions().len(), 2);
    assert!(
        compacted_versions
            .versions()
            .iter()
            .any(|version| version.is_latest().unwrap_or(false))
    );

    client
        .put_object()
        .bucket(&bucket)
        .key("logs/d.txt")
        .body(ByteStream::from_static(b"d"))
        .send()
        .await
        .expect("post-compaction PUT should succeed");
    client
        .delete_object()
        .bucket(&bucket)
        .key("logs/nested/c.txt")
        .send()
        .await
        .expect("post-compaction DELETE should succeed");

    let overlay_listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("logs/")
        .delimiter("/")
        .send()
        .await
        .expect("LIST should merge compacted directory segment and active journal");
    let overlay_keys: Vec<_> = overlay_listing
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    assert_eq!(overlay_keys, vec!["logs/a.txt", "logs/d.txt"]);
    assert!(
        overlay_listing.common_prefixes().is_empty(),
        "post-compaction delete marker should remove now-empty nested prefix"
    );

    let deleted_get = client
        .get_object()
        .bucket(&bucket)
        .key("logs/b.txt")
        .send()
        .await;
    assert!(
        deleted_get.is_err(),
        "delete marker sealed during compaction must remain current"
    );
}

// Internal-only: directly compacts object metadata while holding a live in-process reader.
#[tokio::test]
async fn test_s3_active_get_survives_object_metadata_compaction() {
    let cluster = shared_default_test_cluster().await;

    let app_name = unique_test_name("s3-active-compact");
    let (client_id, client_secret) = create_app(&cluster, &app_name).await;
    grant_storage_tenant_owner_for_test(&cluster, &app_name).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = unique_test_name("s3-active-compact");
    let key = "large/active-read.bin";
    let object_len = LARGE_OBJECT_RANGE_SPLIT_BYTES + 257;
    let payload: Vec<u8> = (0..object_len)
        .map(|index| ((index * 31 + 17) % 251) as u8)
        .collect();

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("large S3 PUT should succeed");

    let get = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("active S3 GET should start");
    let mut reader = get.body.into_async_read();
    let mut first = vec![0_u8; 64 * 1024];
    reader
        .read_exact(&mut first)
        .await
        .expect("read first chunk before compaction");
    assert_eq!(first.as_slice(), &payload[..first.len()]);

    let bucket_record = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    cluster.states[0]
        .persistence
        .compact_object_metadata(bucket_record.id)
        .await
        .unwrap()
        .expect("object metadata compaction should seal a manifest");

    let mut observed = first;
    reader
        .read_to_end(&mut observed)
        .await
        .expect("active GET should drain after compaction");
    assert_eq!(observed, payload);

    let post_compaction_get = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("subsequent GET after active-read compaction should succeed");
    let post_compaction_body = post_compaction_get
        .body
        .collect()
        .await
        .expect("collect subsequent compacted body")
        .into_bytes();
    assert_eq!(post_compaction_body.as_ref(), observed.as_slice());
}

// Internal-only: custom compaction thresholds plus exact task, lease, and CoreMeta checks.
#[tokio::test]
async fn test_s3_writes_trigger_worker_metadata_compaction() {
    let mut cluster = isolated_test_cluster_with_config(
        "worker metadata compaction test needs custom compaction thresholds",
        &["auto-compact-region"],
        |config| {
            config.object_metadata_compaction_frame_threshold = 2;
            config.object_metadata_compaction_bytes_threshold = 0;
            config.task_lease_ttl_secs = 60;
        },
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let app_name = unique_test_name("s3-auto-compact");
    let (client_id, client_secret) = create_app(&cluster, &app_name).await;
    grant_storage_tenant_owner_for_test(&cluster, &app_name).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = unique_test_name("s3-auto-compact");

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("auto/a.txt")
        .body(ByteStream::from_static(b"automatic compaction"))
        .send()
        .await
        .expect("S3 PUT should schedule object metadata compaction");

    let bucket_record = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    let completed_task = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let tasks = cluster.states[0]
                .persistence
                .list_tasks_page(None, 1_000)
                .await
                .unwrap()
                .tasks;
            if let Some(task) = tasks.iter().find(|task| {
                task.task_type == anvil_core::tasks::TaskType::ObjectMetadataCompaction
                    && task.payload == serde_json::json!({ "bucket_id": bucket_record.id })
                    && task.status == anvil_core::tasks::TaskStatus::Completed
            }) {
                break task.clone();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "worker did not complete object metadata compaction task in time; tasks={tasks:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let manifest_row_key = anvil::core_store::core_meta_tuple_key(&[
        anvil::core_store::CoreMetaTuplePart::Utf8("object-metadata-manifest"),
        anvil::core_store::CoreMetaTuplePart::I64(1),
        anvil::core_store::CoreMetaTuplePart::I64(bucket_record.id),
    ])
    .unwrap();
    assert!(
        anvil::core_store::CoreMetaStore::open(cluster.states[0].storage.core_store_meta_path())
            .unwrap()
            .get(
                anvil::core_store::CF_OBJECT_HEADS,
                anvil::core_store::TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
                &manifest_row_key,
            )
            .unwrap()
            .is_some(),
        "worker-completed compaction should publish an object metadata manifest CoreMeta row"
    );
    let lease = cluster.states[0]
        .persistence
        .read_task_execution_lease(completed_task.id)
        .await
        .unwrap()
        .expect("completed compaction task should have a task lease");
    assert_eq!(lease.partition_family, "object_metadata");
    assert_eq!(lease.checkpoint_cursor, lease.source_cursor);

    let get = client
        .get_object()
        .bucket(&bucket)
        .key("auto/a.txt")
        .send()
        .await
        .expect("GET should survive worker compaction");
    let bytes = get
        .body
        .collect()
        .await
        .expect("collect compacted body")
        .into_bytes();
    assert_eq!(bytes.as_ref(), b"automatic compaction");

    let listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("auto/")
        .send()
        .await
        .expect("LIST should survive worker compaction");
    assert_eq!(listing.contents().len(), 1);
    assert_eq!(listing.contents()[0].key(), Some("auto/a.txt"));
}

#[tokio::test]
async fn test_s3_put_triggers_full_text_index_build() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-index").await;

    let client = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("s3-index");
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "body".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/s3-indexed.txt")
        .body(ByteStream::from_static(
            b"s3 writes should flow into full text indexing",
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "body".to_string(),
                    query_text: "full text indexing".to_string(),
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
                &actor.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/s3-indexed.txt")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response = indexed.expect("S3 object should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
}

#[tokio::test]
async fn test_s3_put_metadata_field_triggers_full_text_index_build() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-metadata-index").await;

    let client = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("s3-metadata-index");
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "owner".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "metadata_field",
                    "field": "owner"
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/s3-metadata.txt")
        .metadata("owner", "alice portfolio")
        .body(ByteStream::from_static(
            b"body intentionally does not contain the indexed owner",
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "owner".to_string(),
                    query_text: "alice portfolio".to_string(),
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
                &actor.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/s3-metadata.txt")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        indexed.expect("S3 metadata field should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/s3-metadata.txt");
}

#[tokio::test]
async fn test_s3_put_personaldb_table_column_triggers_full_text_index_build() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-personaldb-column-index").await;

    let client = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("s3-personaldb-column-index");
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "row-name".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "rows/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "personaldb_table_column",
                    "table": "items",
                    "column": "name"
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("rows/items/1.json")
        .content_type("application/json")
        .body(ByteStream::from_static(
            br#"{"table_name":"items","columns":{"id":1,"name":"alpha repair order"}}"#,
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "row-name".to_string(),
                    query_text: "alpha repair".to_string(),
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
                &actor.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "rows/items/1.json")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response = indexed
        .expect("S3 PersonalDB table column should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "rows/items/1.json");
}

#[tokio::test]
async fn test_s3_put_media_transcript_triggers_full_text_index_build() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-media-index").await;

    let client = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("s3-media-index");
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "media".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "media/"}).to_string(),
                extractor_json: serde_json::json!({"source": "media_transcript"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("media/audio/clip.bin")
        .content_type("audio/mpeg")
        .body(ByteStream::from_static(
            b"\x00\x01deterministic audio bytes",
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "media".to_string(),
                    query_text: "audio media object".to_string(),
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
                &actor.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "media/audio/clip.bin")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        indexed.expect("S3 media transcript should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "media/audio/clip.bin");
}

#[tokio::test]
async fn test_s3_put_triggers_vector_index_build() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-vector-index").await;

    let client = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("s3-vector-index");
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "embedding".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_json_vector",
                    "caller_supplied",
                    "test-explicit-vector",
                    2,
                )
                .to_string(),

                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/s3-vector.json")
        .body(ByteStream::from_static(
            br#"{"vector":[0.0,1.0],"source_start":2,"source_len":16}"#,
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "embedding".to_string(),
                    query_text: String::new(),
                    query_vector: vec![0.0, 1.0],
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
                &actor.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/s3-vector.json")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        indexed.expect("S3 object vector should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::Vector as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/s3-vector.json");
}
