use super::*;

#[tokio::test]
async fn test_grant_and_revoke_access() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "grant-revoke").await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("grant-bucket");
    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    add_bearer(&mut create_bucket, &actor.token);
    bucket_client.create_bucket(create_bucket).await.unwrap();

    // Delegation is non-escalating: the granter must already hold the
    // capability it delegates, in addition to policy management authority.
    let tenant_resource = format!("tenant:{}", actor.tenant_id);
    let granter = cluster
        .create_actor_in_tenant(
            actor.tenant_id,
            "granter-app",
            &[
                ("policy:grant", tenant_resource.as_str()),
                ("policy:revoke", tenant_resource.as_str()),
                ("bucket:read", bucket_name.as_str()),
            ],
        )
        .await;

    let grantee = cluster
        .create_actor_in_tenant(actor.tenant_id, "grantee-app", &[])
        .await;

    let granter_token = granter.token;

    let grantee_token =
        get_token(&actor.grpc_addr, &grantee.client_id, &grantee.client_secret).await;

    let resource = bucket_name.clone();

    // 2. Grant access
    let mut grant_req = Request::new(GrantAccessRequest {
        grantee_app_id: grantee.app_name.clone(),
        resource: resource.clone(),
        action: "bucket:read".to_string(),
    });
    grant_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", granter_token).parse().unwrap(),
    );
    auth_client.grant_access(grant_req).await.unwrap();

    // 3. Verify grantee can use the granted relationship.
    let mut allowed_list = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    add_bearer(&mut allowed_list, &grantee_token);
    object_client.list_objects(allowed_list).await.unwrap();

    // 4. Revoke access
    let mut revoke_req = Request::new(RevokeAccessRequest {
        grantee_app_id: grantee.app_name,
        resource: resource.clone(),
        action: "bucket:read".to_string(),
    });
    revoke_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", granter_token).parse().unwrap(),
    );
    auth_client.revoke_access(revoke_req).await.unwrap();

    // 5. Existing tokens identify the principal only; authorization is checked
    // at request time, so the revoked relationship must deny the same token.
    let mut denied_list = Request::new(ListObjectsRequest {
        bucket_name,
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    add_bearer(&mut denied_list, &grantee_token);
    let denied = object_client.list_objects(denied_list).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_authz_tuple_write_check_and_watch() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-tuple-watch").await;
    let schema_revision = prepare_docker_default_authz_realm(&cluster, &actor).await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut watch_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let mut write_add = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: "add".to_string(),
        reason: "grant viewer".to_string(),
        scope: None,
    });
    write_add.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let add = auth_client
        .write_authz_tuple(write_add)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(add.revision, schema_revision + 1);
    assert_eq!(add.zookie, format!("authz:{}", add.revision));
    assert!(!add.record_hash.is_empty());

    let mut watch_req = Request::new(WatchAuthzTupleLogRequest {
        after_revision: 0,
        namespace: "document".to_string(),
        scope: None,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = watch_client
        .watch_authz_tuple_log(watch_req)
        .await
        .unwrap()
        .into_inner();
    let watched_add = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(watched_add.revision, add.revision);
    assert_eq!(watched_add.namespace, "document");
    assert_eq!(watched_add.operation, "add");
    let envelope = watched_add.envelope.as_ref().expect("authz watch envelope");
    assert_eq!(envelope.watch_stream_id, "authz_tuple_log");
    assert_eq!(envelope.partition_family, "authz_tuple");
    assert_eq!(envelope.cursor_low, watched_add.revision);
    assert_eq!(envelope.authz_revision, watched_add.revision);
    assert_eq!(envelope.record_kind, "authz_tuple");
    assert!(envelope.object_ref.contains("document:alpha#viewer"));
    assert!(!envelope.payload_hash.is_empty());

    let mut check_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        scope: None,
    });
    check_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let allowed = auth_client
        .check_permission(check_req)
        .await
        .unwrap()
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.revision, add.revision);

    let mut exact_add_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "exact".to_string(),
        zookie: add.zookie.clone(),
        scope: None,
    });
    exact_add_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let exact_add = auth_client
        .check_permission(exact_add_req)
        .await
        .unwrap()
        .into_inner();
    assert!(exact_add.allowed);
    assert_eq!(exact_add.revision, add.revision);

    let mut write_remove = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: "remove".to_string(),
        reason: "revoke viewer".to_string(),
        scope: None,
    });
    write_remove.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let remove = auth_client
        .write_authz_tuple(write_remove)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(remove.revision, add.revision + 1);

    let watched_remove = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(watched_remove.revision, remove.revision);
    assert_eq!(watched_remove.operation, "remove");

    let mut check_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        scope: None,
    });
    check_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let denied = auth_client
        .check_permission(check_req)
        .await
        .unwrap()
        .into_inner();
    assert!(!denied.allowed);
    assert_eq!(denied.revision, remove.revision);

    let mut at_least_add_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "at_least".to_string(),
        zookie: add.zookie.clone(),
        scope: None,
    });
    at_least_add_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let at_least_add = auth_client
        .check_permission(at_least_add_req)
        .await
        .unwrap()
        .into_inner();
    assert!(!at_least_add.allowed);
    assert_eq!(at_least_add.revision, remove.revision);

    // Historical point checks are served from materialized segments. Wait on
    // the lag stream rather than assuming the asynchronous worker has already
    // caught up when the tuple mutation returns.
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
    tokio::time::timeout(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT, async {
        loop {
            let event = lag_stream
                .next()
                .await
                .expect("derived-lag stream ended before materialization caught up")
                .expect("derived-lag stream returned an error");
            if event.derived_index_id == DEFAULT_DERIVED_USERSET_INDEX_ID
                && event.processed_revision >= remove.revision
            {
                break;
            }
        }
    })
    .await
    .expect("authorization materialization did not catch up");

    let mut exact_add_after_remove_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "exact".to_string(),
        zookie: add.zookie.clone(),
        scope: None,
    });
    exact_add_after_remove_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let exact_add_after_remove = auth_client
        .check_permission(exact_add_after_remove_req)
        .await
        .unwrap()
        .into_inner();
    assert!(exact_add_after_remove.allowed);
    assert_eq!(exact_add_after_remove.revision, add.revision);

    let mut unavailable_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "exact".to_string(),
        zookie: "authz:999".to_string(),
        scope: None,
    });
    unavailable_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let unavailable = auth_client
        .check_permission(unavailable_req)
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_authz_batch_read_and_list_operations() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-batch-read").await;
    let schema_revision = prepare_docker_default_authz_realm(&cluster, &actor).await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let mut write = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![
            authz_mutation("document", "alpha", "viewer", "user", "alice", "add"),
            authz_mutation("document", "beta", "viewer", "user", "alice", "add"),
            authz_mutation("document", "beta", "viewer", "user", "alice", "remove"),
            authz_mutation("document", "alpha", "editor", "user", "bob", "add"),
        ],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut write, &token);
    let written = auth_client
        .write_authz_tuples(write)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.results.len(), 4);
    assert!(
        written
            .results
            .iter()
            .all(|result| result.revision == schema_revision + 1)
    );
    assert_eq!(written.revision, schema_revision + 1);
    assert_eq!(written.zookie, format!("authz:{}", written.revision));

    let mut first_page = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        page_size: 1,
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut first_page, &token);
    let first_page = auth_client
        .read_authz_tuples(first_page)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.tuples.len(), 1);
    assert_eq!(first_page.tuples[0].object_id, "alpha");
    assert!(!first_page.next_page_token.is_empty());

    let mut wrong_filter_page = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        object_id: "beta".to_string(),
        page_size: 1,
        page_token: first_page.next_page_token.clone(),
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut wrong_filter_page, &token);
    let wrong_filter = auth_client
        .read_authz_tuples(wrong_filter_page)
        .await
        .unwrap_err();
    assert_eq!(wrong_filter.code(), tonic::Code::InvalidArgument);

    let mut second_page = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        page_size: 1,
        page_token: first_page.next_page_token,
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut second_page, &token);
    let second_page = auth_client
        .read_authz_tuples(second_page)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.tuples.len(), 1);
    assert!(second_page.next_page_token.is_empty());

    let mut checks = Request::new(CheckPermissionsRequest {
        checks: vec![
            check_permission_request("document", "alpha", "viewer", "user", "alice", "latest", ""),
            check_permission_request("document", "beta", "viewer", "user", "alice", "latest", ""),
        ],
    });
    add_bearer(&mut checks, &token);
    let checks = auth_client
        .check_permissions(checks)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        checks
            .results
            .iter()
            .map(|result| result.allowed)
            .collect::<Vec<_>>(),
        vec![true, false]
    );
    assert_eq!(checks.revision, written.revision);

    let mut objects = Request::new(ListAuthzObjectsRequest {
        namespace: "document".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut objects, &token);
    let objects = auth_client
        .list_authz_objects(objects)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(objects.object_ids, vec!["alpha".to_string()]);

    let mut subjects = Request::new(ListAuthzSubjectsRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "editor".to_string(),
        subject_kind: "user".to_string(),
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut subjects, &token);
    let subjects = auth_client
        .list_authz_subjects(subjects)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(subjects.subjects.len(), 1);
    assert_eq!(subjects.subjects[0].subject_id, "bob");
}

#[tokio::test]
async fn test_authz_batch_watch_and_current_pagination_rejects_stale_revision() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-batch-watch").await;
    let schema_revision = prepare_docker_default_authz_realm(&cluster, &actor).await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut first_batch = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![
            authz_mutation("document", "alpha", "viewer", "user", "alice", "add"),
            authz_mutation("document", "beta", "viewer", "user", "bob", "add"),
            authz_mutation("document", "gamma", "viewer", "user", "charlie", "add"),
        ],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut first_batch, &token);
    let first_batch = auth_client
        .write_authz_tuples(first_batch)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_batch.revision, schema_revision + 1);
    assert_eq!(first_batch.results.len(), 3);

    let mut watch_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchAuthzTupleLogRequest {
        after_revision: 0,
        namespace: "document".to_string(),
        scope: None,
    });
    add_bearer(&mut watch_req, &token);
    let mut stream = watch_client
        .watch_authz_tuple_log(watch_req)
        .await
        .unwrap()
        .into_inner();

    let mut watched = Vec::new();
    for _ in 0..first_batch.results.len() {
        watched.push(
            tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(watched.len(), 3);
    assert!(
        watched
            .iter()
            .all(|event| event.revision == first_batch.revision),
        "batch watch events must become visible only as one committed revision"
    );
    assert_eq!(
        watched
            .iter()
            .map(|event| event.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta", "gamma"]
    );

    let mut first_page = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        page_size: 1,
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut first_page, &token);
    let first_page = auth_client
        .read_authz_tuples(first_page)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.revision, first_batch.revision);
    assert!(!first_page.next_page_token.is_empty());

    let mut second_batch = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document", "delta", "viewer", "user", "dana", "add",
        )],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut second_batch, &token);
    let second_batch = auth_client
        .write_authz_tuples(second_batch)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_batch.revision, first_batch.revision + 1);

    let mut revision_bound_page = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        page_size: 1,
        page_token: first_page.next_page_token,
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut revision_bound_page, &token);
    let revision_bound_page = auth_client
        .read_authz_tuples(revision_bound_page)
        .await
        .unwrap_err();
    assert_eq!(revision_bound_page.code(), tonic::Code::FailedPrecondition);
    assert!(
        revision_bound_page
            .message()
            .contains("historical authz list reads are not supported")
    );

    let mut latest_page = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        page_size: 100,
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut latest_page, &token);
    let latest_page = auth_client
        .read_authz_tuples(latest_page)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(latest_page.revision, second_batch.revision);
    assert!(
        latest_page
            .tuples
            .iter()
            .any(|tuple| tuple.object_id == "delta")
    );
}

// This test stays in-process because it asserts the storage-layer authz revision
// does not advance after rejected batches via cluster.states.persistence.
#[tokio::test]
async fn test_authz_tuple_batch_failure_is_atomic() {
    let mut cluster = isolated_test_cluster(
        "asserts failed authz batches do not advance a fresh tenant revision",
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

    let mut invalid_batch = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![
            authz_mutation("document", "alpha", "viewer", "user", "alice", "add"),
            AuthzTupleMutation {
                namespace: "document".to_string(),
                object_id: "beta".to_string(),
                relation: "viewer".to_string(),
                subject_kind: "user".to_string(),
                subject_id: "alice".to_string(),
                caveat_hash: "not-a-hex32-caveat".to_string(),
                operation: "add".to_string(),
                reason: "invalid".to_string(),
                scope: None,
            },
        ],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut invalid_batch, &token);
    let err = auth_client
        .write_authz_tuples(invalid_batch)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        cluster.states[0]
            .persistence
            .latest_authz_revision(1)
            .await
            .unwrap(),
        0,
        "failed authz batches must not advance the tenant revision"
    );

    let mut unsafe_component_batch = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![
            authz_mutation("document", "alpha", "viewer", "user", "alice", "add"),
            authz_mutation("bad/slash", "beta", "viewer", "user", "alice", "add"),
        ],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut unsafe_component_batch, &token);
    let err = auth_client
        .write_authz_tuples(unsafe_component_batch)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        cluster.states[0]
            .persistence
            .latest_authz_revision(1)
            .await
            .unwrap(),
        0,
        "failed authz batches with unsafe components must not advance the tenant revision"
    );

    let tenant_id = cluster.states[0]
        .persistence
        .get_tenant_by_name("default")
        .await
        .unwrap()
        .expect("default test tenant exists")
        .id;
    let schema_revision =
        bind_default_authz_schema(&cluster.grpc_addrs[0], tenant_id, &token).await;

    let mut valid_batch = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![
            authz_mutation("document", "alpha", "viewer", "user", "alice", "add"),
            authz_mutation("document", "beta", "viewer", "user", "alice", "add"),
        ],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut valid_batch, &token);
    let valid = auth_client
        .write_authz_tuples(valid_batch)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(valid.revision, schema_revision + 1);
    assert!(
        valid
            .results
            .iter()
            .all(|result| result.revision == valid.revision)
    );
}

#[tokio::test]
async fn test_conditional_authz_batch_idempotency_and_revision_conflicts() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-batch-idempotency").await;
    prepare_docker_default_authz_realm(&cluster, &actor).await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mutations = vec![
        authz_mutation("document", "alpha", "viewer", "user", "alice", "add"),
        authz_mutation("document", "beta", "editor", "user", "bob", "add"),
    ];

    let mut baseline_request = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document",
            "baseline",
            "viewer",
            "user",
            "baseline-user",
            "add",
        )],
        scope: None,
        operation_id: Some("establish-current-revision".to_string()),
        expected_revision: None,
    });
    add_bearer(&mut baseline_request, &actor.token);
    let baseline = auth_client
        .write_authz_tuples(baseline_request)
        .await
        .unwrap()
        .into_inner();

    let mut first_request = Request::new(WriteAuthzTuplesRequest {
        mutations: mutations.clone(),
        scope: None,
        operation_id: Some("grant-initial-access".to_string()),
        expected_revision: Some(baseline.revision),
    });
    add_bearer(&mut first_request, &actor.token);
    let first = auth_client
        .write_authz_tuples(first_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.revision, baseline.revision + 1);

    let mut advance_request = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document", "gamma", "viewer", "user", "carol", "add",
        )],
        scope: None,
        operation_id: Some("grant-follow-up-access".to_string()),
        expected_revision: Some(first.revision),
    });
    add_bearer(&mut advance_request, &actor.token);
    let advanced = auth_client
        .write_authz_tuples(advance_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(advanced.revision, first.revision + 1);

    let mut retry_request = Request::new(WriteAuthzTuplesRequest {
        mutations: mutations.clone(),
        scope: None,
        operation_id: Some("grant-initial-access".to_string()),
        expected_revision: Some(baseline.revision),
    });
    add_bearer(&mut retry_request, &actor.token);
    let retry = auth_client
        .write_authz_tuples(retry_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retry, first,
        "an exact retry must return the first response"
    );

    let mut changed_mutations = mutations;
    changed_mutations[0].object_id = "changed".to_string();
    let mut changed_request = Request::new(WriteAuthzTuplesRequest {
        mutations: changed_mutations,
        scope: None,
        operation_id: Some("grant-initial-access".to_string()),
        expected_revision: Some(baseline.revision),
    });
    add_bearer(&mut changed_request, &actor.token);
    let changed = auth_client
        .write_authz_tuples(changed_request)
        .await
        .unwrap_err();
    assert_eq!(changed.code(), tonic::Code::Aborted);
    assert_eq!(changed.message(), "AuthzOperationConflict");

    let mut stale_request = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document", "delta", "viewer", "user", "dana", "add",
        )],
        scope: None,
        operation_id: Some("stale-grant".to_string()),
        expected_revision: Some(first.revision),
    });
    add_bearer(&mut stale_request, &actor.token);
    let stale = auth_client
        .write_authz_tuples(stale_request)
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::Aborted);
    assert!(stale.message().contains("AuthzRevisionConflict"));

    let mut oversized_id_request = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document", "epsilon", "viewer", "user", "erin", "add",
        )],
        scope: None,
        operation_id: Some("x".repeat(129)),
        expected_revision: None,
    });
    add_bearer(&mut oversized_id_request, &actor.token);
    let oversized = auth_client
        .write_authz_tuples(oversized_id_request)
        .await
        .unwrap_err();
    assert_eq!(oversized.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_authz_accepts_arbitrary_safe_subject_kind() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-safe-subject").await;
    let schema_revision = prepare_docker_default_authz_realm(&cluster, &actor).await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let mut write = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document", "doc-1", "viewer", "folder", "folder-1", "add",
        )],
        scope: None,
        ..Default::default()
    });
    add_bearer(&mut write, &token);
    let written = auth_client
        .write_authz_tuples(write)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.revision, schema_revision + 1);

    let mut check = Request::new(check_permission_request(
        "document", "doc-1", "viewer", "folder", "folder-1", "latest", "",
    ));
    add_bearer(&mut check, &token);
    let check = auth_client
        .check_permission(check)
        .await
        .unwrap()
        .into_inner();
    assert!(check.allowed);

    let mut read = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        object_id: "doc-1".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "folder".to_string(),
        subject_id: "folder-1".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        page_size: 10,
        page_token: String::new(),
        scope: None,
    });
    add_bearer(&mut read, &token);
    let read = auth_client
        .read_authz_tuples(read)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.tuples.len(), 1);
    assert_eq!(read.tuples[0].subject_kind, "folder");
}
