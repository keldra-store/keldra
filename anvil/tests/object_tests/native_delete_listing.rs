use super::*;

fn writer_segment_catalog_tuple_key(family: &str, scope: &str, segment_ref: &str) -> Vec<u8> {
    anvil::core_store::core_meta_tuple_key(&[
        anvil::core_store::CoreMetaTuplePart::Utf8("writer-segment"),
        anvil::core_store::CoreMetaTuplePart::Utf8(family),
        anvil::core_store::CoreMetaTuplePart::Utf8(scope),
        anvil::core_store::CoreMetaTuplePart::Utf8(segment_ref),
    ])
    .unwrap()
}

async fn write_remote_bucket_locator_for_node0(
    cluster: &TestCluster,
    bucket_name: &str,
    region: &str,
) {
    let state = &cluster.states[0];
    let signing_key = hex::decode(&state.config.anvil_secret_encryption_key).unwrap();
    let bucket_name = BucketName::canonicalize(bucket_name).unwrap();
    let bucket_name_string = bucket_name.as_str().to_string();
    let locator = BucketLocatorDescriptor::active(
        MeshId::new("test-mesh").unwrap(),
        TenantId::new("1").unwrap(),
        bucket_name,
        BucketId::new(format!("bucket-{bucket_name_string}-{region}")).unwrap(),
        RegionName::new(region.to_string()).unwrap(),
        CellId::new("remote-cell").unwrap(),
        "regional-primary",
        &format!("objects/1/{bucket_name_string}/{region}/"),
        "2026-07-02T00:00:00Z",
    )
    .unwrap();
    let partition = locator.partition();
    let control_partition_id = mesh_directory::control_partition_id(
        RoutingRecordFamily::BucketLocator.stream_family(),
        &partition,
    );
    let recovering = acquire_partition_recovery(
        &state.storage,
        PartitionRecoveryAcquire {
            partition_family: mesh_directory::CONTROL_PARTITION_FAMILY.to_string(),
            partition_id: control_partition_id,
            owner_node_id: state.config.node_id.clone(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 1,
        },
        &signing_key,
    )
    .await
    .unwrap();
    let ready = publish_partition_ready(
        &state.storage,
        &recovering.partition_family,
        &recovering.partition_id,
        &state.config.node_id,
        recovering.fence_token,
        0,
        &hex::encode([0; 32]),
        2,
        &signing_key,
    )
    .await
    .unwrap();
    mesh_directory::write_bucket_locator(
        &state.storage,
        &locator,
        MeshControlWriteAuthority {
            permit: &ready.write_permit().unwrap(),
            signing_key: &signing_key,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn native_object_routes_apply_cross_region_policy_before_local_metadata() {
    let cluster = shared_default_test_cluster().await;
    let bucket_name = unique_test_name("remote-route");

    write_remote_bucket_locator_for_node0(&cluster, &bucket_name, "test-region-2").await;

    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let err = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: "any.txt".to_string(),
                version_id: None,
                range: None,

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(
        err.metadata().get("x-anvil-bucket-region").unwrap(),
        "test-region-2"
    );
    assert_eq!(
        err.metadata().get("x-anvil-cross-region-action").unwrap(),
        "redirect"
    );
}

#[tokio::test]
async fn native_object_routes_report_proxy_required_as_unavailable_when_proxy_is_absent() {
    let mut cluster = isolated_test_cluster_with_config(
        "uses a custom cross-region proxy-required routing policy",
        &["test-region-1"],
        |config| {
            config.cross_region_routing_policy =
                anvil::routing::CrossRegionRoutingPolicy::ProxyRequired;
        },
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let bucket_name = unique_test_name("remote-proxy");
    write_remote_bucket_locator_for_node0(&cluster, &bucket_name, "test-region-2").await;

    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let err = object_client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name: bucket_name.clone(),
                prefix: String::new(),
                start_after: String::new(),
                delimiter: String::new(),
                max_keys: 100,

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        err.metadata().get("x-anvil-bucket-region").unwrap(),
        "test-region-2"
    );
    assert_eq!(
        err.metadata().get("x-anvil-cross-region-action").unwrap(),
        "proxy_unavailable"
    );
}

#[tokio::test]
async fn test_native_mutations_require_valid_context() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_object_test_actor(&cluster, "native-mutations-require-valid-context").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = unique_test_name("native-context");

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut missing_context_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "missing-context.txt",
        b"missing",
        None,
    )));
    missing_context_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(missing_context_req)
        .await
        .expect_err("native mutation without context must fail");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("Missing native mutation context"));

    let mut wrong_principal = native_mutation_context(&actor, bucket_id, "wrong-principal");
    wrong_principal.principal = "other-app".to_string();
    let mut wrong_principal_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "wrong-principal.txt",
        b"wrong",
        Some(wrong_principal),
    )));
    wrong_principal_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(wrong_principal_req)
        .await
        .expect_err("native mutation with mismatched principal must fail");
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(err.message().contains("principal mismatch"));

    let mut wrong_tenant = native_mutation_context(&actor, bucket_id, "wrong-tenant");
    wrong_tenant.tenant_id = 2;
    let mut wrong_tenant_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "wrong-tenant.txt",
        b"wrong",
        Some(wrong_tenant),
    )));
    wrong_tenant_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(wrong_tenant_req)
        .await
        .expect_err("native mutation with mismatched tenant must fail");
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(err.message().contains("tenant mismatch"));

    let mut wrong_bucket = native_mutation_context(&actor, bucket_id + 1, "wrong-bucket");
    let mut wrong_bucket_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "wrong-bucket.txt",
        b"wrong",
        Some(wrong_bucket.clone()),
    )));
    wrong_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(wrong_bucket_req)
        .await
        .expect_err("native mutation with mismatched bucket must fail");
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(err.message().contains("bucket mismatch"));

    wrong_bucket.bucket_id = bucket_id;
    wrong_bucket.request_id.clear();
    let mut blank_field_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "blank-request-id.txt",
        b"wrong",
        Some(wrong_bucket),
    )));
    blank_field_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(blank_field_req)
        .await
        .expect_err("native mutation with blank request_id must fail");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("request_id"));

    let mut stale_zookie = native_mutation_context(&actor, bucket_id, "stale-zookie");
    stale_zookie.authz_zookie_optional = "authz:999999".to_string();
    let mut stale_zookie_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "stale-zookie.txt",
        b"wrong",
        Some(stale_zookie),
    )));
    stale_zookie_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(stale_zookie_req)
        .await
        .expect_err("native mutation with unavailable authz revision must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("AuthzRevisionUnavailable"));
}

#[tokio::test]
async fn test_native_object_mutation_preconditions_are_enforced() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "native-object-mutation-preconditions-are-enforce")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = unique_test_name("native-precond");
    let object_key = "docs/preconditioned.txt";

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

    let first = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"first",
        native_mutation_context_with_precondition(&actor, bucket_id, "first", "not_exists"),
    )
    .await
    .expect("not_exists precondition should allow initial object creation");
    assert_native_mutation_response!(first);

    let duplicate = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"duplicate",
        native_mutation_context_with_precondition(&actor, bucket_id, "duplicate", "not_exists"),
    )
    .await
    .expect_err("not_exists precondition must reject an existing object");
    assert_eq!(duplicate.code(), Code::FailedPrecondition);
    assert!(duplicate.message().contains("precondition failed"));

    let wrong_etag = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"wrong-etag",
        native_mutation_context_with_precondition(
            &actor,
            bucket_id,
            "wrong-etag",
            "etag:not-current",
        ),
    )
    .await
    .expect_err("etag precondition must reject a mismatched object etag");
    assert_eq!(wrong_etag.code(), Code::FailedPrecondition);

    let second = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"second",
        native_mutation_context_with_precondition(
            &actor,
            bucket_id,
            "matching-etag",
            &format!("etag:\"{}\"", first.etag),
        ),
    )
    .await
    .expect("etag precondition should allow matching object replacement");
    assert_native_mutation_response!(second);

    let third = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"third",
        native_mutation_context_with_precondition(
            &actor,
            bucket_id,
            "matching-version",
            &format!("version:{}", second.version_id),
        ),
    )
    .await
    .expect("version precondition should allow matching object replacement");
    assert_native_mutation_response!(third);

    let unsupported = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"unsupported",
        native_mutation_context_with_precondition(&actor, bucket_id, "unsupported", "after:123"),
    )
    .await
    .expect_err("unsupported native precondition syntax must fail");
    assert_eq!(unsupported.code(), Code::InvalidArgument);
    assert!(unsupported.message().contains("Unsupported"));

    let delete_response = object_client
        .delete_object(authorized(
            DeleteObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                mutation_context: Some(native_mutation_context_with_precondition(
                    &actor,
                    bucket_id,
                    "delete-existing",
                    "exists",
                )),
            },
            &token,
        ))
        .await
        .expect("exists precondition should allow deleting current object")
        .into_inner();
    assert!(!delete_response.mutation_id.is_empty());
    assert!(!delete_response.record_hash.is_empty());
    assert!(delete_response.watch_cursor > third.watch_cursor);
    assert!(delete_response.delete_marker);

    let recreated = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"recreated",
        native_mutation_context_with_precondition(&actor, bucket_id, "recreated", "not_exists"),
    )
    .await
    .expect("not_exists should treat the current delete marker as absent");
    assert_native_mutation_response!(recreated);
}

#[tokio::test]
async fn test_native_object_mutation_idempotency_replays_without_duplicate_mutation() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "native-object-mutation-idempotency-replays-witho")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = unique_test_name("native-idem");
    let object_key = "docs/idempotent.txt";

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

    let put_context = native_mutation_context(&actor, bucket_id, "idempotent-put");
    let put_idempotency_key = put_context.idempotency_key.clone();
    let first = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"first-payload",
        put_context.clone(),
    )
    .await
    .expect("first idempotent put should succeed");
    let replayed = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"second-payload-must-not-be-written",
        put_context,
    )
    .await
    .expect("second idempotent put should replay");
    assert_eq!(replayed, first);

    let downloaded = get_object_bytes_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        Some(first.version_id.clone()),
    )
    .await;
    assert_eq!(downloaded, b"first-payload");

    let versions = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name: bucket_name.clone(),
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 10,
                version_id_marker: String::new(),

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(
        versions.len(),
        1,
        "idempotent replay must not add a version"
    );
    assert_eq!(versions[0].version_id, first.version_id);

    let mut reused_context = native_mutation_context(&actor, bucket_id, "reused-target");
    reused_context.idempotency_key = put_idempotency_key;
    let conflict = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        "docs/other-target.txt",
        b"other",
        reused_context,
    )
    .await
    .expect_err("idempotency key reuse against a different target must fail");
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    assert!(conflict.message().contains("different mutation target"));

    let delete_context = native_mutation_context(&actor, bucket_id, "idempotent-delete");
    let delete_first = object_client
        .delete_object(authorized(
            DeleteObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                mutation_context: Some(delete_context.clone()),
            },
            &token,
        ))
        .await
        .expect("first idempotent delete should succeed")
        .into_inner();
    let delete_replayed = object_client
        .delete_object(authorized(
            DeleteObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                mutation_context: Some(delete_context),
            },
            &token,
        ))
        .await
        .expect("second idempotent delete should replay")
        .into_inner();
    assert_eq!(delete_replayed, delete_first);
    assert!(delete_replayed.delete_marker);

    let versions_after_delete = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name,
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 10,
                version_id_marker: String::new(),

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(
        versions_after_delete.len(),
        2,
        "idempotent delete replay must not add another delete marker"
    );
    assert_eq!(
        versions_after_delete
            .iter()
            .filter(|version| version.is_delete_marker)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_repair_rebuilds_missing_directory_segment_from_metadata_journal() {
    let cluster = shared_default_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "repair-rebuilds-missing-directory-segment-from-m")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let persistence = &cluster.states[0].persistence;
    let bucket_name = unique_test_name("dir-repair");
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
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
    let bucket = persistence
        .get_bucket_by_name(actor.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("service-created bucket should be visible to persistence");
    assert_eq!(bucket.id, bucket_id);
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    put_native_object_bytes(
        &actor,
        &mut object_client,
        &token,
        &bucket_name,
        bucket.id,
        "docs/a.txt",
        b"directory-a".to_vec(),
        Some("text/plain"),
    )
    .await;
    put_native_object_bytes(
        &actor,
        &mut object_client,
        &token,
        &bucket_name,
        bucket.id,
        "docs/b.txt",
        b"directory-b".to_vec(),
        Some("text/plain"),
    )
    .await;
    let sealed = persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes directory segment");
    let directory_segment_ref = sealed
        .directory_ref
        .strip_prefix("coresegment:")
        .expect("sealed directory segment uses CoreStore segment refs");
    anvil::core_store::CoreMetaStore::open(cluster.states[0].storage.core_store_meta_path())
        .unwrap()
        .delete(
            anvil::core_store::CF_MATERIALISATION,
            anvil::core_store::TABLE_WRITER_SEGMENT_ROW,
            &writer_segment_catalog_tuple_key(
                "object_metadata_segment",
                &format!("segment/{directory_segment_ref}"),
                directory_segment_ref,
            ),
        )
        .expect("remove directory segment CoreMeta row to force repair");

    let mut repair_client = RepairServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let report = repair_client
        .repair_directory_index(authorized(
            RepairDirectoryIndexRequest {
                bucket_name: bucket_name.clone(),
                rebuild: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "needs_repair");
    assert_eq!(report.reason, "DirectoryIndexInvalid");
    assert_eq!(report.expected_entry_count, 2);
    assert!(report.finding.is_some());

    let rebuilt = repair_client
        .repair_directory_index(authorized(
            RepairDirectoryIndexRequest {
                bucket_name: bucket_name.clone(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rebuilt.status, "rebuilt_directory_index");
    assert_eq!(rebuilt.reason, "DirectoryIndexInvalid");
    assert_eq!(rebuilt.expected_entry_count, 2);
    assert!(!rebuilt.rebuilt_manifest_hash.is_empty());

    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();
    let listed = object_client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name,
                prefix: "docs/".to_string(),
                start_after: String::new(),
                max_keys: 10,
                delimiter: String::new(),

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        listed
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt", "docs/b.txt"]
    );
}

#[tokio::test]
async fn test_delete_object_creates_delete_marker() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_object_test_actor(&cluster, "delete-object-creates-delete-marker").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("delete-object");
    let object_key = "test-delete-object".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    // 1. Put an object
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
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"delete me".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let put_res = object_client
        .put_object(put_req)
        .await
        .unwrap()
        .into_inner();
    assert!(put_res.watch_cursor > 0);

    // 2. Verify it exists
    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.objects.len(), 1);

    // 3. Delete the object
    let mut del_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        mutation_context: Some(native_mutation_context(&actor, bucket_id, "delete-object")),
    });
    del_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let delete_res = object_client
        .delete_object(del_req)
        .await
        .unwrap()
        .into_inner();
    assert!(delete_res.watch_cursor > put_res.watch_cursor);
    assert!(delete_res.delete_marker);
    assert!(!delete_res.version_id.is_empty());
    assert!(!delete_res.mutation_id.is_empty());
    assert!(!delete_res.record_hash.is_empty());

    // 4. Verify it is gone from listings (soft deleted)
    let mut list_req_after_delete = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    });
    list_req_after_delete.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res_after_delete = object_client
        .list_objects(list_req_after_delete)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res_after_delete.objects.len(), 0);

    // 5. Verify versions retain the original object plus a latest delete marker.
    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),

        ..Default::default()
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 2);
    assert!(versions[0].is_delete_marker);
    assert!(versions[0].is_latest);
    assert!(!versions[1].is_delete_marker);
    assert!(!versions[1].is_latest);
}

#[tokio::test]
async fn test_delete_object_specific_version_removes_only_that_version() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "delete-object-specific-version-removes-only-that")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("delete-version");
    let object_key = "versioned-object".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let first_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
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
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"v1".to_vec(),
            )),
        },
    ];
    let mut first_put_req = Request::new(tokio_stream::iter(first_chunks));
    first_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first_put = object_client
        .put_object(first_put_req)
        .await
        .unwrap()
        .into_inner();

    let second_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
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
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"v2".to_vec(),
            )),
        },
    ];
    let mut second_put_req = Request::new(tokio_stream::iter(second_chunks));
    second_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second_put = object_client
        .put_object(second_put_req)
        .await
        .unwrap()
        .into_inner();

    let mut first_page_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 1,
        version_id_marker: String::new(),

        ..Default::default()
    });
    first_page_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first_page = object_client
        .list_object_versions(first_page_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.versions.len(), 1);
    assert_eq!(first_page.versions[0].version_id, second_put.version_id);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker, object_key);
    assert_eq!(first_page.next_version_id_marker, second_put.version_id);

    let mut second_page_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: first_page.next_key_marker,
        max_keys: 1,
        version_id_marker: first_page.next_version_id_marker,

        ..Default::default()
    });
    second_page_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second_page = object_client
        .list_object_versions(second_page_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].version_id, first_put.version_id);
    assert!(!second_page.is_truncated);
    assert!(second_page.next_key_marker.is_empty());
    assert!(second_page.next_version_id_marker.is_empty());

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(first_put.version_id.clone()),
        mutation_context: Some(native_mutation_context(&actor, bucket_id, "delete-object")),
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.delete_object(delete_req).await.unwrap();

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),

        ..Default::default()
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, second_put.version_id);
    assert!(versions[0].is_latest);
    assert!(!versions[0].is_delete_marker);
}

#[tokio::test]
async fn test_get_object_without_version_id_returns_latest_version() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "get-object-without-version-id-returns-laversion").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("latest-get");
    let object_key = "docs/versioned.txt";

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

    let first = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"version-one",
        native_mutation_context(&actor, bucket_id, "put-first"),
    )
    .await
    .unwrap();
    let second = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"version-two",
        native_mutation_context(&actor, bucket_id, "put-second"),
    )
    .await
    .unwrap();
    let latest = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"version-three-latest",
        native_mutation_context(&actor, bucket_id, "put-latest"),
    )
    .await
    .unwrap();

    let head = object_client
        .head_object(authorized(
            HeadObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(head.version_id, latest.version_id);

    let versions = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name: bucket_name.clone(),
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 100,
                version_id_marker: String::new(),

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(
        versions
            .iter()
            .map(|version| version.version_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            latest.version_id.as_str(),
            second.version_id.as_str(),
            first.version_id.as_str()
        ]
    );
    assert!(versions[0].is_latest);
    assert!(versions[1..].iter().all(|version| !version.is_latest));

    let (metadata, downloaded) = get_object_metadata_and_bytes_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        None,
    )
    .await;
    assert_eq!(metadata.version_id, latest.version_id);
    assert_eq!(metadata.content_length, "version-three-latest".len() as i64);
    assert_eq!(downloaded, b"version-three-latest");

    let (first_metadata, first_downloaded) = get_object_metadata_and_bytes_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        Some(first.version_id.clone()),
    )
    .await;
    assert_eq!(first_metadata.version_id, first.version_id);
    assert_eq!(first_downloaded, b"version-one");
}

#[tokio::test]
async fn test_utf8_object_keys_with_spaces_round_trip() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_object_test_actor(&cluster, "utf8-object-keys-with-spaces-round-trip").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("utf8-keys");
    let object_key = "folder/my café document 📄.txt".to_string();
    let payload = b"utf8 object key payload".to_vec();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
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
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(payload.clone())),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let put_res = object_client
        .put_object(put_req)
        .await
        .unwrap()
        .into_inner();
    assert!(!put_res.version_id.is_empty());

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "folder/".to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 10,

        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.objects.len(), 1);
    assert_eq!(list_res.objects[0].key, object_key);

    let literal_prefix = "literal/a%_";
    let literal_key = "literal/a%_object.txt";
    let wildcard_decoy_key = "literal/abc-object.txt";
    for (key, body) in [
        (literal_key, b"literal wildcard key".to_vec()),
        (wildcard_decoy_key, b"decoy key".to_vec()),
    ] {
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Metadata(
                    ObjectMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: key.to_string(),
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
                data: Some(anvil_api::put_object_request::Data::Chunk(body)),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        object_client.put_object(put_req).await.unwrap();
    }

    let mut wildcard_prefix_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: literal_prefix.to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 10,

        ..Default::default()
    });
    wildcard_prefix_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let wildcard_prefix_list = object_client
        .list_objects(wildcard_prefix_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(wildcard_prefix_list.objects.len(), 1);
    assert_eq!(wildcard_prefix_list.objects[0].key, literal_key);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key,
        version_id: None,
        range: None,

        ..Default::default()
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = object_client
        .get_object(get_req)
        .await
        .unwrap()
        .into_inner();
    let mut downloaded = Vec::new();
    while let Some(chunk) = stream.next().await {
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            downloaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(downloaded, payload);
}

#[tokio::test]
async fn test_listing_omits_reserved_internal_object_keys() {
    let cluster = shared_default_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "listing-omits-reserved-internal-object-keys").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("hides-internal");
    let visible_key = "visible/object.txt".to_string();
    let reserved_key = "_anvil/authz/tuples".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: visible_key.clone(),
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
                b"visible".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(actor.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    let bucket_id = bucket.id;
    let tenant_id = bucket.tenant_id;
    let visible_object = cluster.states[0]
        .persistence
        .get_object(bucket_id, &visible_key)
        .await
        .unwrap()
        .expect("visible object metadata should exist");
    cluster.states[0]
        .persistence
        .create_object(
            tenant_id,
            bucket_id,
            &reserved_key,
            &visible_object.content_hash,
            visible_object.size,
            "reserved-etag",
            None,
            None,
            visible_object.shard_map.clone(),
            None,
            None,
        )
        .await
        .unwrap();

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let objects = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner()
        .objects;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, visible_key);

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name,
        prefix: String::new(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),

        ..Default::default()
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].key, visible_key);
}
