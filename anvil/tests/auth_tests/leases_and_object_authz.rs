use super::*;

#[tokio::test]
async fn test_coordination_task_lease_grpc_flow() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "lease-flow").await;

    let token = actor.token.clone();
    let mut coordination_client = CoordinationServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let task_id = unique_test_name("posthorn-delivery");
    let mut acquire = Request::new(AcquireTaskLeaseRequest {
        task_id: task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "44".repeat(32),
        owner_label: "posthorn-worker-a".to_string(),
        source_cursor_low: 10,
        source_cursor_high: 0,
        requested_ttl_nanos: 60_000_000_000,
    });
    add_bearer(&mut acquire, &token);
    let acquired = coordination_client
        .acquire_task_lease(acquire)
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("lease");
    assert_eq!(acquired.task_id, task_id);
    assert_eq!(acquired.fence_token, 1);
    assert_eq!(acquired.source_cursor_low, 10);
    assert_eq!(acquired.owner_label, "posthorn-worker-a");
    assert_eq!(acquired.owner_tenant_id, actor.tenant_id);
    assert_eq!(acquired.owner_principal_kind, "app");
    assert_eq!(acquired.owner_principal_id.as_str(), actor.app_id.as_str());
    assert!(!acquired.owner_actor_instance_id.is_empty());
    assert!(!acquired.lease_hash.is_empty());

    let mut checkpoint = Request::new(CheckpointTaskLeaseRequest {
        task_id: task_id.to_string(),
        fence_token: acquired.fence_token,
        checkpoint_cursor_low: 42,
        checkpoint_cursor_high: 0,
        expected_root_generation: acquired.root_generation,
        expected_lease_epoch: acquired.lease_epoch,
        expected_expires_at_nanos: acquired.expires_at_nanos,
        expected_lease_hash: acquired.lease_hash.clone(),
    });
    add_bearer(&mut checkpoint, &token);
    let checkpointed = coordination_client
        .checkpoint_task_lease(checkpoint)
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("lease");
    assert_eq!(checkpointed.checkpoint_cursor_low, 42);
    assert_eq!(checkpointed.fence_token, acquired.fence_token);

    let mut read = Request::new(ReadTaskLeaseRequest {
        task_id: task_id.to_string(),
    });
    add_bearer(&mut read, &token);
    let read = coordination_client
        .read_task_lease(read)
        .await
        .unwrap()
        .into_inner();
    assert!(read.found);
    assert_eq!(read.lease.expect("lease").checkpoint_cursor_low, 42);

    let mut commit = Request::new(CommitTaskLeaseRequest {
        task_id: task_id.to_string(),
        fence_token: checkpointed.fence_token,
        committed_cursor_low: 50,
        committed_cursor_high: 0,
        expected_root_generation: checkpointed.root_generation,
        expected_lease_epoch: checkpointed.lease_epoch,
        expected_expires_at_nanos: checkpointed.expires_at_nanos,
        expected_lease_hash: checkpointed.lease_hash.clone(),
    });
    add_bearer(&mut commit, &token);
    let committed = coordination_client
        .commit_task_lease(commit)
        .await
        .unwrap()
        .into_inner();
    assert!(committed.committed);
    assert_eq!(
        committed
            .previous_lease
            .expect("committed lease")
            .checkpoint_cursor_low,
        50
    );

    let mut read_committed = Request::new(ReadTaskLeaseRequest {
        task_id: task_id.to_string(),
    });
    add_bearer(&mut read_committed, &token);
    assert!(
        !coordination_client
            .read_task_lease(read_committed)
            .await
            .unwrap()
            .into_inner()
            .found
    );
}

// This test stays in-process because it needs a per-test task lease TTL config
// that the shared Docker cluster cannot change for one test.
#[tokio::test]
async fn test_coordination_task_lease_security_invariants() {
    let mut cluster = isolated_test_cluster_with_config(
        "uses a custom task lease TTL and asserts cross-tenant lease security invariants",
        &["test-region-1"],
        |config| {
            config.task_lease_ttl_secs = 30;
        },
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let (lease_app_a_id, lease_app_a_client, lease_app_a_secret) =
        create_app_with_id(&cluster, "lease-app-a").await;
    grant_policy(
        &cluster,
        "lease-app-a",
        "coordination:lease_write",
        "leases",
    )
    .await;
    grant_policy(&cluster, "lease-app-a", "coordination:lease_read", "leases").await;
    let token_a = get_token(
        &cluster.grpc_addrs[0],
        &lease_app_a_client,
        &lease_app_a_secret,
    )
    .await;

    let (_lease_app_b_id, lease_app_b_client, lease_app_b_secret) =
        create_app_with_id(&cluster, "lease-app-b").await;
    grant_policy(
        &cluster,
        "lease-app-b",
        "coordination:lease_write",
        "leases",
    )
    .await;
    let token_b = get_token(
        &cluster.grpc_addrs[0],
        &lease_app_b_client,
        &lease_app_b_secret,
    )
    .await;

    let tenant_b_id = create_tenant(&cluster, "lease-tenant-b").await;
    let (tenant_b_app_id, tenant_b_client, tenant_b_secret) = cluster
        .create_application_with_id(&tenant_b_id, "lease-app-tenant-b")
        .await;
    cluster
        .grant_application_policy(
            &tenant_b_id,
            "lease-app-tenant-b",
            "coordination:lease_write",
            "leases",
        )
        .await;
    cluster
        .grant_application_policy(
            &tenant_b_id,
            "lease-app-tenant-b",
            "coordination:lease_read",
            "leases",
        )
        .await;
    let token_tenant_b =
        get_token(&cluster.grpc_addrs[0], &tenant_b_client, &tenant_b_secret).await;

    let (_lease_admin_id, lease_admin_client, lease_admin_secret) =
        create_app_with_id(&cluster, "lease-admin").await;
    grant_policy(
        &cluster,
        "lease-admin",
        "coordination:lease_admin",
        "leases",
    )
    .await;
    let admin_token = get_token(
        &cluster.grpc_addrs[0],
        &lease_admin_client,
        &lease_admin_secret,
    )
    .await;

    let mut client = CoordinationServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let task_id = "posthorn-security-invariants";
    let mut acquire = Request::new(AcquireTaskLeaseRequest {
        task_id: task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "55".repeat(32),
        owner_label: "shared-worker-label".to_string(),
        source_cursor_low: 7,
        source_cursor_high: 0,
        requested_ttl_nanos: 600_000_000_000,
    });
    add_bearer(&mut acquire, &token_a);
    let first = client
        .acquire_task_lease(acquire)
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("lease");
    assert_eq!(first.owner_label, "shared-worker-label");
    assert_eq!(first.owner_principal_id.as_str(), lease_app_a_id.as_str());
    assert!(first.expires_at_nanos - first.acquired_at_nanos <= 30_000_000_000);

    let mut wrong_owner_checkpoint = Request::new(CheckpointTaskLeaseRequest {
        task_id: task_id.to_string(),
        fence_token: first.fence_token,
        checkpoint_cursor_low: 8,
        checkpoint_cursor_high: 0,
        expected_root_generation: first.root_generation,
        expected_lease_epoch: first.lease_epoch,
        expected_expires_at_nanos: first.expires_at_nanos,
        expected_lease_hash: first.lease_hash.clone(),
    });
    add_bearer(&mut wrong_owner_checkpoint, &token_b);
    let err = client
        .checkpoint_task_lease(wrong_owner_checkpoint)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(err.message(), "LeaseOwnerMismatch");

    let mut same_label_acquire = Request::new(AcquireTaskLeaseRequest {
        task_id: task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "55".repeat(32),
        owner_label: "shared-worker-label".to_string(),
        source_cursor_low: 8,
        source_cursor_high: 0,
        requested_ttl_nanos: 1_000_000_000,
    });
    add_bearer(&mut same_label_acquire, &token_b);
    let err = client
        .acquire_task_lease(same_label_acquire)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(err.message(), "LeaseHeld");

    let mut unauthorized_release = Request::new(ForceReleaseTaskLeaseRequest {
        task_id: task_id.to_string(),
    });
    add_bearer(&mut unauthorized_release, &token_a);
    let err = client
        .force_release_task_lease(unauthorized_release)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    let mut force_release = Request::new(ForceReleaseTaskLeaseRequest {
        task_id: task_id.to_string(),
    });
    add_bearer(&mut force_release, &admin_token);
    let release = client
        .force_release_task_lease(force_release)
        .await
        .unwrap()
        .into_inner();
    assert!(release.released);
    assert_eq!(
        release.previous_lease.expect("previous").fence_token,
        first.fence_token
    );

    let mut read_released = Request::new(ReadTaskLeaseRequest {
        task_id: task_id.to_string(),
    });
    add_bearer(&mut read_released, &token_a);
    assert!(
        !client
            .read_task_lease(read_released)
            .await
            .unwrap()
            .into_inner()
            .found
    );

    let stale_task_id = "posthorn-stale-fence";
    let mut short_acquire = Request::new(AcquireTaskLeaseRequest {
        task_id: stale_task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "66".repeat(32),
        owner_label: "lease-app-a".to_string(),
        source_cursor_low: 1,
        source_cursor_high: 0,
        requested_ttl_nanos: 1,
    });
    add_bearer(&mut short_acquire, &token_a);
    let stale_first = client
        .acquire_task_lease(short_acquire)
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("lease");
    tokio::time::sleep(Duration::from_millis(2)).await;

    let mut renewed = Request::new(AcquireTaskLeaseRequest {
        task_id: stale_task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "66".repeat(32),
        owner_label: "lease-app-a".to_string(),
        source_cursor_low: 2,
        source_cursor_high: 0,
        requested_ttl_nanos: 1_000_000_000,
    });
    add_bearer(&mut renewed, &token_a);
    let stale_second = client
        .acquire_task_lease(renewed)
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("lease");
    assert_eq!(stale_second.fence_token, stale_first.fence_token + 1);

    let mut stale_checkpoint = Request::new(CheckpointTaskLeaseRequest {
        task_id: stale_task_id.to_string(),
        fence_token: stale_first.fence_token,
        checkpoint_cursor_low: 3,
        checkpoint_cursor_high: 0,
        expected_root_generation: stale_first.root_generation,
        expected_lease_epoch: stale_first.lease_epoch,
        expected_expires_at_nanos: stale_first.expires_at_nanos,
        expected_lease_hash: stale_first.lease_hash.clone(),
    });
    add_bearer(&mut stale_checkpoint, &token_a);
    let err = client
        .checkpoint_task_lease(stale_checkpoint)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(err.message(), "StaleFence");

    let mut stale_commit = Request::new(CommitTaskLeaseRequest {
        task_id: stale_task_id.to_string(),
        fence_token: stale_first.fence_token,
        committed_cursor_low: 3,
        committed_cursor_high: 0,
        expected_root_generation: stale_first.root_generation,
        expected_lease_epoch: stale_first.lease_epoch,
        expected_expires_at_nanos: stale_first.expires_at_nanos,
        expected_lease_hash: stale_first.lease_hash.clone(),
    });
    add_bearer(&mut stale_commit, &token_a);
    let err = client.commit_task_lease(stale_commit).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(err.message(), "StaleFence");

    let tenant_b_task_id = "tenant-b-isolated";
    let mut tenant_b_acquire = Request::new(AcquireTaskLeaseRequest {
        task_id: tenant_b_task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "77".repeat(32),
        owner_label: "tenant-b-worker".to_string(),
        source_cursor_low: 1,
        source_cursor_high: 0,
        requested_ttl_nanos: 1_000_000_000,
    });
    add_bearer(&mut tenant_b_acquire, &token_tenant_b);
    let tenant_b_lease = client
        .acquire_task_lease(tenant_b_acquire)
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("lease");
    assert_eq!(tenant_b_lease.owner_tenant_id.to_string(), tenant_b_id);
    assert_eq!(
        tenant_b_lease.owner_principal_id.as_str(),
        tenant_b_app_id.as_str()
    );

    let mut tenant_a_checkpoint = Request::new(CheckpointTaskLeaseRequest {
        task_id: tenant_b_task_id.to_string(),
        fence_token: tenant_b_lease.fence_token,
        checkpoint_cursor_low: 2,
        checkpoint_cursor_high: 0,
        expected_root_generation: tenant_b_lease.root_generation,
        expected_lease_epoch: tenant_b_lease.lease_epoch,
        expected_expires_at_nanos: tenant_b_lease.expires_at_nanos,
        expected_lease_hash: tenant_b_lease.lease_hash.clone(),
    });
    add_bearer(&mut tenant_a_checkpoint, &token_a);
    assert!(
        client
            .checkpoint_task_lease(tenant_a_checkpoint)
            .await
            .is_err()
    );

    let mut tenant_b_read = Request::new(ReadTaskLeaseRequest {
        task_id: tenant_b_task_id.to_string(),
    });
    add_bearer(&mut tenant_b_read, &token_tenant_b);
    let tenant_b_read = client
        .read_task_lease(tenant_b_read)
        .await
        .unwrap()
        .into_inner();
    assert!(tenant_b_read.found);
    assert_eq!(
        tenant_b_read
            .lease
            .expect("tenant b lease")
            .owner_tenant_id
            .to_string(),
        tenant_b_id
    );

    let mut client_one = CoordinationServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut client_two = CoordinationServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let concurrent_task_id = "posthorn-concurrent-acquire";
    let mut acquire_one = Request::new(AcquireTaskLeaseRequest {
        task_id: concurrent_task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "88".repeat(32),
        owner_label: "same-worker-label".to_string(),
        source_cursor_low: 1,
        source_cursor_high: 0,
        requested_ttl_nanos: 1_000_000_000,
    });
    add_bearer(&mut acquire_one, &token_a);
    let mut acquire_two = Request::new(AcquireTaskLeaseRequest {
        task_id: concurrent_task_id.to_string(),
        task_kind: "posthorn_delivery".to_string(),
        partition_family: "posthorn_queue".to_string(),
        partition_id: "88".repeat(32),
        owner_label: "same-worker-label".to_string(),
        source_cursor_low: 1,
        source_cursor_high: 0,
        requested_ttl_nanos: 1_000_000_000,
    });
    add_bearer(&mut acquire_two, &token_b);

    let (one, two) = tokio::join!(
        client_one.acquire_task_lease(acquire_one),
        client_two.acquire_task_lease(acquire_two)
    );
    let winners = one.is_ok() as u8 + two.is_ok() as u8;
    assert_eq!(winners, 1, "concurrent lease acquire must have one winner");
    let loser = if one.is_err() {
        one.unwrap_err()
    } else {
        two.unwrap_err()
    };
    assert_eq!(loser.code(), tonic::Code::FailedPrecondition);
    assert_eq!(loser.message(), "LeaseHeld");
}

// This test stays in-process because it verifies rejected caveats do not
// advance the storage-layer authz revision via cluster.states.persistence.
#[tokio::test]
async fn test_authz_tuple_rejects_invalid_caveat_hash_before_writing() {
    let mut cluster = isolated_test_cluster(
        "asserts invalid caveats leave a fresh authz revision at zero",
        &["test-region-1"],
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let token = cluster.token.clone();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut invalid_write = Request::new(WriteAuthzTupleRequest {
            context: None,
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: "not-a-hex32-caveat".to_string(),
        operation: "add".to_string(),
        reason: "invalid caveat".to_string(),
        scope: None,
    });
    add_bearer(&mut invalid_write, &token);
    let status = auth_client
        .write_authz_tuple(invalid_write)
        .await
        .expect_err("invalid caveat hash must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        cluster.states[0]
            .persistence
            .latest_authz_revision(1)
            .await
            .unwrap(),
        0,
        "invalid caveat input must not append an authz revision"
    );

    let mut invalid_check = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: "not-a-hex32-caveat".to_string(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        scope: None,
    });
    add_bearer(&mut invalid_check, &token);
    let status = auth_client
        .check_permission(invalid_check)
        .await
        .expect_err("invalid caveat hash must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_authz_permission_resolves_nested_usersets() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-nested-usersets").await;
    let schema_revision = prepare_docker_default_authz_realm(&cluster, &actor).await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let tuples = [
        write_authz_tuple_request("group", "engineering", "member", "user", "alice", "add"),
        write_authz_tuple_request(
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        write_authz_tuple_request(
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ];
    let mut zookies = Vec::new();
    for tuple in tuples {
        let mut request = Request::new(tuple);
        add_bearer(&mut request, &token);
        let response = auth_client
            .write_authz_tuple(request)
            .await
            .unwrap()
            .into_inner();
        zookies.push(response.zookie);
    }

    let mut allowed_request = Request::new(check_permission_request(
        "document", "alpha", "viewer", "user", "alice", "latest", "",
    ));
    add_bearer(&mut allowed_request, &token);
    let allowed = auth_client
        .check_permission(allowed_request)
        .await
        .unwrap()
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.explanation_ref, "tuple_or_userset_match");

    let mut denied_request = Request::new(check_permission_request(
        "document", "alpha", "viewer", "user", "bob", "latest", "",
    ));
    add_bearer(&mut denied_request, &token);
    let denied = auth_client
        .check_permission(denied_request)
        .await
        .unwrap()
        .into_inner();
    assert!(!denied.allowed);
    assert_eq!(denied.explanation_ref, "no_current_tuple_or_userset");

    let mut remove_request = Request::new(write_authz_tuple_request(
        "folder",
        "platform",
        "viewer",
        "userset",
        "group/engineering#member",
        "remove",
    ));
    add_bearer(&mut remove_request, &token);
    let remove = auth_client
        .write_authz_tuple(remove_request)
        .await
        .unwrap()
        .into_inner();

    let mut exact_before_remove = Request::new(check_permission_request(
        "document",
        "alpha",
        "viewer",
        "user",
        "alice",
        "exact",
        &zookies[2],
    ));
    add_bearer(&mut exact_before_remove, &token);
    let exact_before_remove = auth_client
        .check_permission(exact_before_remove)
        .await
        .unwrap()
        .into_inner();
    assert!(exact_before_remove.allowed);

    let mut latest_after_remove = Request::new(check_permission_request(
        "document", "alpha", "viewer", "user", "alice", "latest", "",
    ));
    add_bearer(&mut latest_after_remove, &token);
    let latest_after_remove = auth_client
        .check_permission(latest_after_remove)
        .await
        .unwrap()
        .into_inner();
    assert!(!latest_after_remove.allowed);
    assert_eq!(latest_after_remove.zookie, remove.zookie);

    let mut lag_request = Request::new(WatchAuthzDerivedLagRequest {
        derived_index_id: DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    add_bearer(&mut lag_request, &token);
    let mut lag_stream = auth_client
        .watch_authz_derived_lag(lag_request)
        .await
        .unwrap()
        .into_inner();
    let target_revision = schema_revision + 4;
    let mut previous_revision = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), lag_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.derived_index_id, DEFAULT_DERIVED_USERSET_INDEX_ID);
        assert_eq!(event.derived_index_kind, "userset");
        assert!(event.processed_revision > previous_revision);
        assert!(event.processed_revision <= target_revision);
        assert!(event.latest_revision >= event.processed_revision);
        assert_eq!(
            event.revision_lag,
            event.latest_revision - event.processed_revision
        );
        previous_revision = event.processed_revision;
        if previous_revision == target_revision {
            assert_eq!(event.latest_revision, target_revision);
            assert_eq!(event.revision_lag, 0);
            break;
        }
    }
}

#[tokio::test]
async fn test_object_read_uses_relationship_authorization_before_streaming_bytes() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "relationship-read").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("rel-read");
    let object_key = "private/report.txt".to_string();
    let payload = b"relationship authorized object".to_vec();

    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    add_bearer(&mut create_bucket, &token);
    let bucket_id = bucket_client
        .create_bucket(create_bucket)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let put_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    mutation_context: Some(native_mutation_context(
                        actor.tenant_id,
                        bucket_id,
                        &actor.app_id,
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
                payload.clone(),
            )),
        },
    ];
    let mut put_request = Request::new(tokio_stream::iter(put_chunks));
    add_bearer(&mut put_request, &token);
    object_client.put_object(put_request).await.unwrap();

    let reader = cluster
        .create_actor_in_tenant(actor.tenant_id, "rel-reader", &[])
        .await;
    let reader_token = reader.token.clone();

    let mut denied_get = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    add_bearer(&mut denied_get, &reader_token);
    let denied = object_client.get_object(denied_get).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    grant_docker_actor_policy(
        &cluster,
        &reader,
        "object:read",
        &format!("{bucket_name}/{object_key}"),
    )
    .await;

    let mut allowed_get = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    add_bearer(&mut allowed_get, &reader_token);
    let mut stream = object_client
        .get_object(allowed_get)
        .await
        .unwrap()
        .into_inner();
    let first = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        first.data,
        Some(anvil::anvil_api::get_object_response::Data::Metadata(_))
    ));
    let second = stream.next().await.unwrap().unwrap();
    let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = second.data else {
        panic!("second get_object response must be payload bytes");
    };
    assert_eq!(bytes, payload);
}
