use super::*;

#[tokio::test]
async fn test_mutation_batch_put_object_publishes_small_json_without_upload_stall() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_object_test_actor(&cluster, "mutation-batch-put-object-small-json").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("batch-put");
    let object_key = "/markets/pl".to_string();
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: actor.region.clone(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let response = tokio::time::timeout(
        Duration::from_secs(120),
        object_client.mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: bucket_name.clone(),
                mutation_context: Some(native_mutation_context(
                    &actor,
                    bucket_id,
                    "batch-put-small-json",
                )),
                precondition: None,
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::PutObject(
                        MutationBatchPutObject {
                            object_key: object_key.clone(),
                            payload: br#"{"market":"pl","ready":true}"#.to_vec(),
                            content_type: Some("application/json".to_string()),
                            user_metadata_json: serde_json::json!({
                                "boundaries": {
                                    "market": "pl",
                                    "legal_realm": "eu",
                                    "provider": "cacydil"
                                }
                            })
                            .to_string(),
                            storage_class: None,
                        },
                    )),
                }],
            },
            &token,
        )),
    )
    .await
    .expect("mutation batch put_object must not stall on scratch upload")
    .unwrap()
    .into_inner();

    assert_eq!(response.operation_receipts.len(), 1);
    assert_eq!(response.operation_receipts[0].operation, "put_object");
    assert_eq!(response.operation_receipts[0].object_key, object_key);

    let head = object_client
        .head_object(authorized(
            HeadObjectRequest {
                bucket_name,
                object_key,
                version_id: None,
                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(head.content_type, "application/json");
}

#[tokio::test]
async fn test_mutation_batch_rejects_stale_lease_fence_for_state_update() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "mutation-batch-rejects-stale-lease-fence-for-sta")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut coordination_client = CoordinationServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("fenced-batch");
    let object_key = "queue/item-1.json".to_string();
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: actor.region.clone(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(&actor, bucket_id, "fenced-seed")),
                content_type: Some("application/json".to_string()),
                user_metadata_json: String::new(),
                storage_class: None,
            },
        )),
    };
    let chunk = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Chunk(
            br#"{"state":{"state":"pending"}}"#.to_vec(),
        )),
    };
    object_client
        .put_object(authorized(
            tokio_stream::iter(vec![metadata, chunk]),
            &token,
        ))
        .await
        .unwrap();

    let task_id = unique_test_name("queue-item");
    let lease = coordination_client
        .acquire_task_lease(authorized(
            AcquireTaskLeaseRequest {
                task_id: task_id.clone(),
                task_kind: "queue_item".to_string(),
                partition_family: "queue".to_string(),
                partition_id: hex::encode([1_u8; 32]),
                owner_label: "worker-a".to_string(),
                source_cursor_low: 0,
                source_cursor_high: 0,
                requested_ttl_nanos: 60_000_000_000,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .lease
        .unwrap();

    let batch = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: bucket_name.clone(),
                mutation_context: Some(native_mutation_context(
                    &actor,
                    bucket_id,
                    "fenced-batch-claim",
                )),
                precondition: Some(WritePrecondition {
                    object_versions: vec![],
                    lease_fence: Some(LeaseFencePrecondition {
                        task_id: task_id.clone(),
                        fence_token: lease.fence_token,
                    }),
                }),
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::PutObject(
                        MutationBatchPutObject {
                            object_key: object_key.clone(),
                            payload: br#"{"state":{"state":"leased"}}"#.to_vec(),
                            content_type: Some("application/json".into()),
                            user_metadata_json: String::new(),
                            storage_class: None,
                        },
                    )),
                }],
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(batch.operation_receipts.len(), 1);

    coordination_client
        .commit_task_lease(authorized(
            anvil_api::CommitTaskLeaseRequest {
                task_id: task_id.clone(),
                fence_token: lease.fence_token,
                committed_cursor_low: 1,
                committed_cursor_high: 0,
                expected_root_generation: lease.root_generation,
                expected_lease_epoch: lease.lease_epoch,
                expected_expires_at_nanos: lease.expires_at_nanos,
                expected_lease_hash: lease.lease_hash.clone(),
            },
            &token,
        ))
        .await
        .unwrap();

    let stale = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: bucket_name.clone(),
                mutation_context: Some(native_mutation_context(
                    &actor,
                    bucket_id,
                    "fenced-batch-stale",
                )),
                precondition: Some(WritePrecondition {
                    object_versions: vec![],
                    lease_fence: Some(LeaseFencePrecondition {
                        task_id: task_id.clone(),
                        fence_token: lease.fence_token,
                    }),
                }),
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::PutObject(
                        MutationBatchPutObject {
                            object_key: object_key.clone(),
                            payload: br#"{"state":{"state":"completed"}}"#.to_vec(),
                            content_type: Some("application/json".into()),
                            user_metadata_json: String::new(),
                            storage_class: None,
                        },
                    )),
                }],
            },
            &token,
        ))
        .await;
    assert!(stale.is_err());
}

#[tokio::test]
async fn test_compare_and_swap_manifest_enforces_expected_revision() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "compare-and-swap-manifest-enforces-expected-revi")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("manifest");
    let manifest_key = "manifests/current.json".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

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

    let mut create_manifest = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: manifest_key.clone(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 1}).to_string(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    create_manifest.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first = object_client
        .compare_and_swap_manifest(create_manifest)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(first);
    assert_eq!(first.revision, 1);
    assert_eq!(first.version_id, "1");
    assert!(!first.manifest_hash.is_empty());

    let mut stale_update = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: manifest_key.clone(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 2}).to_string(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    stale_update.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert!(
        object_client
            .compare_and_swap_manifest(stale_update)
            .await
            .is_err()
    );

    let mut valid_update = Request::new(CompareAndSwapManifestRequest {
        bucket_name,
        manifest_key,
        expected_revision: first.revision,
        manifest_json: serde_json::json!({"generation": 2}).to_string(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    valid_update.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second = object_client
        .compare_and_swap_manifest(valid_update)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(second);
    assert_eq!(second.revision, 2);
    assert_eq!(second.version_id, "2");
    assert_ne!(second.manifest_hash, first.manifest_hash);
    assert!(second.watch_cursor > first.watch_cursor);
}

#[tokio::test]
async fn test_multipart_upload_completes_ordered_parts() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "multipart-upload-completes-ordered-parts").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("multipart");
    let object_key = "multipart.txt".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

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

    let mut initiate_req = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "initiate-multipart",
        )),
    });
    initiate_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let initiate_res = object_client
        .initiate_multipart_upload(initiate_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(initiate_res);
    assert_eq!(initiate_res.version_id, initiate_res.upload_id);
    let upload_id = initiate_res.upload_id;

    let part_payloads = [(1, b"multi".to_vec()), (2, b"part".to_vec())];
    let mut completed_parts = Vec::new();
    for (part_number, payload) in part_payloads {
        let chunks = vec![
            UploadPartRequest {
                data: Some(anvil_api::upload_part_request::Data::Metadata(
                    UploadPartMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        upload_id: upload_id.clone(),
                        part_number,
                        mutation_context: Some(native_mutation_context(
                            &actor,
                            bucket_id,
                            "upload-part",
                        )),
                    },
                )),
            },
            UploadPartRequest {
                data: Some(anvil_api::upload_part_request::Data::Chunk(payload)),
            },
        ];
        let mut upload_req = Request::new(tokio_stream::iter(chunks));
        upload_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let upload_part = object_client
            .upload_part(upload_req)
            .await
            .unwrap()
            .into_inner();
        assert_native_mutation_response!(upload_part);
        assert_eq!(upload_part.version_id, part_number.to_string());
        completed_parts.push(CompleteMultipartPart {
            part_number,
            etag: upload_part.etag,
        });
    }

    let mut complete_req = Request::new(CompleteMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        upload_id,
        parts: completed_parts,
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "complete-multipart",
        )),
    });
    complete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let complete_res = object_client
        .complete_multipart_upload(complete_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(complete_res);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(complete_res.version_id),
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

    assert_eq!(downloaded, b"multipart");
}

#[tokio::test]
async fn test_multipart_abort_returns_mutation_metadata() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "multipart-abort-returns-mutation-metadata").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("mpu-abort");
    let object_key = "aborted.txt".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

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

    let mut initiate_req = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "initiate-multipart",
        )),
    });
    initiate_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let initiate_res = object_client
        .initiate_multipart_upload(initiate_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(initiate_res);

    let mut abort_req = Request::new(AbortMultipartRequest {
        bucket_name,
        object_key,
        upload_id: initiate_res.upload_id.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "abort-multipart",
        )),
    });
    abort_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let abort_res = object_client
        .abort_multipart_upload(abort_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(abort_res);
    assert_eq!(abort_res.version_id, initiate_res.upload_id);
    assert!(abort_res.watch_cursor > initiate_res.watch_cursor);
}

#[tokio::test]
async fn test_compose_object_concatenates_sources_in_order() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "compose-object-concatenates-sources-in-order").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("compose");
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

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

    let sources = vec![
        ("part-a.txt", b"hello ".to_vec()),
        ("part-b.txt", b"compose".to_vec()),
    ];
    let mut source_versions = Vec::new();
    for (key, content) in &sources {
        let metadata = ObjectMetadata {
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
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    metadata,
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    content.clone(),
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
        source_versions.push((key.to_string(), put_res.version_id));
    }

    let mut compose_req = Request::new(ComposeObjectRequest {
        sources: source_versions
            .into_iter()
            .map(|(key, version_id)| ComposeObjectSource {
                bucket_name: bucket_name.clone(),
                object_key: key,
                version_id: Some(version_id),
            })
            .collect(),
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: "composed.txt".to_string(),
        mutation_context: Some(native_mutation_context(&actor, bucket_id, "compose-object")),
    });
    compose_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let compose_res = object_client
        .compose_object(compose_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(compose_res);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: "composed.txt".to_string(),
        version_id: Some(compose_res.version_id),
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
        match chunk.unwrap().data.unwrap() {
            anvil_api::get_object_response::Data::Metadata(metadata) => {
                assert_eq!(metadata.content_length, "hello compose".len() as i64);
            }
            anvil_api::get_object_response::Data::Chunk(bytes) => {
                downloaded.extend_from_slice(&bytes);
            }
        }
    }

    assert_eq!(downloaded, b"hello compose");
}
