use super::*;

fn with_idempotency_key(
    actor: &ObjectTestActor,
    bucket_id: i64,
    tag: &str,
    key: &str,
) -> NativeMutationContext {
    let mut context = native_mutation_context(actor, bucket_id, tag);
    context.idempotency_key = key.to_string();
    context
}

async fn put_with_context(
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
    context: NativeMutationContext,
) -> Result<anvil_api::PutObjectResponse, Status> {
    let chunks = put_object_chunks(bucket_name, object_key, payload, Some(context));
    let mut request = Request::new(tokio_stream::iter(chunks));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client
        .put_object(request)
        .await
        .map(tonic::Response::into_inner)
}

async fn upload_part_with_context(
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    upload_id: &str,
    part_number: i32,
    payload: &[u8],
    context: NativeMutationContext,
) -> Result<anvil_api::UploadPartResponse, Status> {
    let chunks = vec![
        UploadPartRequest {
            data: Some(anvil_api::upload_part_request::Data::Metadata(
                UploadPartMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    upload_id: upload_id.to_string(),
                    part_number,
                    mutation_context: Some(context),
                },
            )),
        },
        UploadPartRequest {
            data: Some(anvil_api::upload_part_request::Data::Chunk(
                payload.to_vec(),
            )),
        },
    ];
    let mut request = Request::new(tokio_stream::iter(chunks));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client
        .upload_part(request)
        .await
        .map(tonic::Response::into_inner)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn implicit_native_retries_reconstruct_results_and_reject_changed_inputs_across_nodes() {
    let mut cluster = isolated_test_cluster_with_config(
        "native retry must cross coordinators in a three-node quorum cluster",
        &["test-region-1", "test-region-1", "test-region-1"],
        |config| config.mvcc_default_durability = "quorum".to_string(),
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let actor = create_object_test_actor(
        &cluster,
        "implicit-native-retries-reconstruct-results-across-nodes",
    )
    .await;
    let endpoint_a = cluster.grpc_addrs[0].clone();
    let endpoint_b = cluster.grpc_addrs[1].clone();
    let mut buckets = BucketServiceClient::connect(endpoint_a.clone())
        .await
        .unwrap();
    let mut first_node = ObjectServiceClient::connect(endpoint_a).await.unwrap();
    let mut second_node = ObjectServiceClient::connect(endpoint_b).await.unwrap();
    let bucket_name = unique_test_name("native-mvcc-retry");
    let bucket_id = buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: actor.region.clone(),
                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let put_key = uuid::Uuid::new_v4().to_string();
    let put_context = with_idempotency_key(&actor, bucket_id, "put-lost-response", &put_key);
    let first_put = put_with_context(
        &mut first_node,
        &actor.token,
        &bucket_name,
        "retry.bin",
        b"durable payload",
        put_context.clone(),
    )
    .await
    .unwrap();
    // Model a lost response by discarding it and retrying through another
    // coordinator. The exact committed response must be reconstructed.
    let retried_put = put_with_context(
        &mut second_node,
        &actor.token,
        &bucket_name,
        "retry.bin",
        b"durable payload",
        put_context.clone(),
    )
    .await
    .unwrap();
    assert_eq!(retried_put, first_put);
    let changed_put = put_with_context(
        &mut second_node,
        &actor.token,
        &bucket_name,
        "retry.bin",
        b"different payload",
        put_context,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            changed_put.code(),
            Code::AlreadyExists | Code::FailedPrecondition
        ),
        "changed-input reuse must be rejected, got {changed_put:?}"
    );

    let cas_key = uuid::Uuid::new_v4().to_string();
    let cas_context = with_idempotency_key(&actor, bucket_id, "cas-lost-response", &cas_key);
    let cas_request = CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: "manifest.json".to_string(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 1}).to_string(),
        mutation_context: Some(cas_context.clone()),
        precondition: None,
    };
    let first_cas = first_node
        .compare_and_swap_manifest(authorized(cas_request.clone(), &actor.token))
        .await
        .unwrap()
        .into_inner();
    let retried_cas = second_node
        .compare_and_swap_manifest(authorized(cas_request, &actor.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried_cas, first_cas);
    let changed_cas = second_node
        .compare_and_swap_manifest(authorized(
            CompareAndSwapManifestRequest {
                bucket_name: bucket_name.clone(),
                manifest_key: "manifest.json".to_string(),
                expected_revision: 0,
                manifest_json: serde_json::json!({"generation": 2}).to_string(),
                mutation_context: Some(cas_context),
                precondition: None,
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        changed_cas.code(),
        Code::AlreadyExists | Code::FailedPrecondition
    ));

    let create_stream = first_node
        .create_append_stream(authorized(
            CreateAppendStreamRequest {
                bucket_name: bucket_name.clone(),
                stream_key: "events".to_string(),
                mutation_context: Some(native_mutation_context(&actor, bucket_id, "create-stream")),
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    let append_key = uuid::Uuid::new_v4().to_string();
    let append_context =
        with_idempotency_key(&actor, bucket_id, "append-lost-response", &append_key);
    let append_request = AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: "events".to_string(),
        stream_id: create_stream.stream_id.clone(),
        payload: b"event payload".to_vec(),
        mutation_context: Some(append_context.clone()),
        content_type: Some("application/octet-stream".to_string()),
        user_metadata_json: String::new(),
        precondition: None,
    };
    let first_append = first_node
        .append_stream_record(authorized(append_request.clone(), &actor.token))
        .await
        .unwrap()
        .into_inner();
    let retried_append = second_node
        .append_stream_record(authorized(append_request, &actor.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried_append, first_append);
    let changed_append = second_node
        .append_stream_record(authorized(
            AppendStreamRecordRequest {
                bucket_name: bucket_name.clone(),
                stream_key: "events".to_string(),
                stream_id: create_stream.stream_id,
                payload: b"different event".to_vec(),
                mutation_context: Some(append_context),
                content_type: Some("application/octet-stream".to_string()),
                user_metadata_json: String::new(),
                precondition: None,
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        changed_append.code(),
        Code::AlreadyExists | Code::FailedPrecondition
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multipart_and_append_payloads_round_trip_from_mvcc_representations() {
    let mut cluster = isolated_test_cluster_with_config(
        "native payload representations must round-trip from erasure shards",
        &["test-region-1", "test-region-1", "test-region-1"],
        |config| config.mvcc_default_durability = "erasure".to_string(),
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let actor = create_object_test_actor(
        &cluster,
        "multipart-and-append-payloads-round-trip-from-mvcc",
    )
    .await;
    let endpoint_a = cluster.grpc_addrs[0].clone();
    let endpoint_b = cluster.grpc_addrs[1].clone();
    let mut buckets = BucketServiceClient::connect(endpoint_a.clone())
        .await
        .unwrap();
    let mut objects = ObjectServiceClient::connect(endpoint_a).await.unwrap();
    let mut retry_node = ObjectServiceClient::connect(endpoint_b).await.unwrap();
    let bucket_name = unique_test_name("native-mvcc-payloads");
    let bucket_id = buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: actor.region.clone(),
                options: None,
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let initiate_key = uuid::Uuid::new_v4().to_string();
    let initiate_context =
        with_idempotency_key(&actor, bucket_id, "multipart-initiate", &initiate_key);
    let initiate_request = InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: "assembled.bin".to_string(),
        mutation_context: Some(initiate_context.clone()),
    };
    let first_initiate = objects
        .initiate_multipart_upload(authorized(initiate_request.clone(), &actor.token))
        .await
        .unwrap()
        .into_inner();
    let retried_initiate = retry_node
        .initiate_multipart_upload(authorized(initiate_request, &actor.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried_initiate, first_initiate);

    let mut completed = Vec::new();
    for (part_number, payload) in [(1, b"mvcc-".as_slice()), (2, b"multipart".as_slice())] {
        let key = uuid::Uuid::new_v4().to_string();
        let context = with_idempotency_key(&actor, bucket_id, "multipart-part", &key);
        let first = upload_part_with_context(
            &mut objects,
            &actor.token,
            &bucket_name,
            "assembled.bin",
            &first_initiate.upload_id,
            part_number,
            payload,
            context.clone(),
        )
        .await
        .unwrap();
        let retry = upload_part_with_context(
            &mut retry_node,
            &actor.token,
            &bucket_name,
            "assembled.bin",
            &first_initiate.upload_id,
            part_number,
            payload,
            context.clone(),
        )
        .await
        .unwrap();
        assert_eq!(retry, first);
        let changed = upload_part_with_context(
            &mut retry_node,
            &actor.token,
            &bucket_name,
            "assembled.bin",
            &first_initiate.upload_id,
            part_number,
            b"changed",
            context,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            changed.code(),
            Code::AlreadyExists | Code::FailedPrecondition
        ));
        completed.push(CompleteMultipartPart {
            part_number,
            etag: first.etag,
        });
    }
    let upload_row = cluster.states[0]
        .persistence
        .get_active_multipart_upload(
            actor.tenant_id,
            bucket_id,
            "assembled.bin",
            uuid::Uuid::parse_str(&first_initiate.upload_id).unwrap(),
        )
        .await
        .unwrap()
        .expect("committed multipart upload row");
    let persisted_parts = cluster.states[0]
        .persistence
        .list_multipart_parts(upload_row.id)
        .await
        .unwrap();
    assert_eq!(persisted_parts.len(), 2);
    assert!(persisted_parts.iter().all(|part| {
        part.object_ref
            .manifest_ref
            .starts_with("anvil-mvcc-target:")
    }));

    let complete_key = uuid::Uuid::new_v4().to_string();
    let complete_context =
        with_idempotency_key(&actor, bucket_id, "multipart-complete", &complete_key);
    let complete_request = CompleteMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: "assembled.bin".to_string(),
        upload_id: first_initiate.upload_id.clone(),
        parts: completed.clone(),
        mutation_context: Some(complete_context.clone()),
    };
    let completed_object = objects
        .complete_multipart_upload(authorized(complete_request.clone(), &actor.token))
        .await
        .unwrap()
        .into_inner();
    let completed_retry = objects
        .complete_multipart_upload(authorized(complete_request, &actor.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(completed_retry, completed_object);
    let changed_complete = objects
        .complete_multipart_upload(authorized(
            CompleteMultipartRequest {
                bucket_name: bucket_name.clone(),
                object_key: "assembled.bin".to_string(),
                upload_id: first_initiate.upload_id,
                parts: completed.into_iter().rev().collect(),
                mutation_context: Some(complete_context),
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        changed_complete.code(),
        Code::AlreadyExists | Code::FailedPrecondition
    ));
    let bytes = get_object_bytes_for_test(
        &mut objects,
        &actor.token,
        &bucket_name,
        "assembled.bin",
        Some(completed_object.version_id),
    )
    .await;
    assert_eq!(bytes, b"mvcc-multipart");

    let stream = objects
        .create_append_stream(authorized(
            CreateAppendStreamRequest {
                bucket_name: bucket_name.clone(),
                stream_key: "mvcc-events".to_string(),
                mutation_context: Some(native_mutation_context(&actor, bucket_id, "mvcc-stream")),
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    objects
        .append_stream_record(authorized(
            AppendStreamRecordRequest {
                bucket_name: bucket_name.clone(),
                stream_key: "mvcc-events".to_string(),
                stream_id: stream.stream_id.clone(),
                payload: b"sharded append payload".to_vec(),
                mutation_context: Some(native_mutation_context(&actor, bucket_id, "mvcc-append")),
                content_type: None,
                user_metadata_json: String::new(),
                precondition: None,
            },
            &actor.token,
        ))
        .await
        .unwrap();
    let stream_row = cluster.states[0]
        .persistence
        .get_active_append_stream(
            actor.tenant_id,
            bucket_id,
            "mvcc-events",
            uuid::Uuid::parse_str(&stream.stream_id).unwrap(),
        )
        .await
        .unwrap()
        .expect("committed append stream");
    let persisted_records = cluster.states[0]
        .persistence
        .list_append_stream_records(&stream_row, None, 10)
        .await
        .unwrap()
        .records;
    assert_eq!(persisted_records.len(), 1);
    assert!(
        persisted_records[0]
            .payload_object_ref
            .manifest_ref
            .starts_with("anvil-mvcc-target:")
    );
    let records = objects
        .read_append_stream(authorized(
            ReadAppendStreamRequest {
                bucket_name,
                stream_key: "mvcc-events".to_string(),
                stream_id: stream.stream_id,
                after_sequence: 0,
                limit: 10,
                include_payload: true,
                ..Default::default()
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"sharded append payload");
}
