use super::*;

#[tokio::test]
async fn test_object_list_and_versions_filter_entries_by_read_relationship() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "object-list-authz").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("rel-list");
    let allowed_key = "docs/allowed.txt".to_string();
    let denied_key = "docs/denied.txt".to_string();
    let visible_nested_key = "visible/nested.txt".to_string();
    let hidden_nested_key = "hidden/nested.txt".to_string();

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

    put_test_object(
        &mut object_client,
        &token,
        actor.tenant_id,
        bucket_id,
        &actor.app_id,
        &bucket_name,
        &allowed_key,
        b"allowed-v1",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        actor.tenant_id,
        bucket_id,
        &actor.app_id,
        &bucket_name,
        &denied_key,
        b"denied",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        actor.tenant_id,
        bucket_id,
        &actor.app_id,
        &bucket_name,
        &allowed_key,
        b"allowed-v2",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        actor.tenant_id,
        bucket_id,
        &actor.app_id,
        &bucket_name,
        &visible_nested_key,
        b"visible",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        actor.tenant_id,
        bucket_id,
        &actor.app_id,
        &bucket_name,
        &hidden_nested_key,
        b"hidden",
    )
    .await;

    let reader = cluster
        .create_actor_in_tenant(
            actor.tenant_id,
            "rel-list-reader",
            &[("object:list", bucket_name.as_str())],
        )
        .await;
    let reader_token = reader.token.clone();

    let mut ungranted_list = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    add_bearer(&mut ungranted_list, &reader_token);
    let ungranted_list = object_client
        .list_objects(ungranted_list)
        .await
        .unwrap()
        .into_inner();
    assert!(ungranted_list.objects.is_empty());
    assert!(ungranted_list.common_prefixes.is_empty());

    for key in [&allowed_key, &visible_nested_key] {
        grant_docker_actor_policy(
            &cluster,
            &reader,
            "object:read",
            &format!("{bucket_name}/{key}"),
        )
        .await;
    }

    let mut list_docs = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "docs/".to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    add_bearer(&mut list_docs, &reader_token);
    let list_docs = object_client
        .list_objects(list_docs)
        .await
        .unwrap()
        .into_inner();
    let listed_keys = list_docs
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(listed_keys, vec![allowed_key.as_str()]);

    let mut delimiter_list = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: "/".to_string(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    add_bearer(&mut delimiter_list, &reader_token);
    let delimiter_list = object_client
        .list_objects(delimiter_list)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(delimiter_list.common_prefixes, vec!["docs/", "visible/"]);
    assert!(
        !delimiter_list
            .common_prefixes
            .iter()
            .any(|prefix| prefix == "hidden/")
    );

    let mut list_versions = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),

        ..Default::default()
    });
    add_bearer(&mut list_versions, &reader_token);
    let list_versions = object_client
        .list_object_versions(list_versions)
        .await
        .unwrap()
        .into_inner();
    let version_keys = list_versions
        .versions
        .iter()
        .map(|version| version.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        version_keys,
        vec![
            allowed_key.as_str(),
            allowed_key.as_str(),
            visible_nested_key.as_str()
        ]
    );
    assert!(!version_keys.contains(&denied_key.as_str()));
    assert!(!version_keys.contains(&hidden_nested_key.as_str()));
    assert!(!list_versions.is_truncated);
}

// This test stays in-process because it injects namespace watch records through
// cluster.states.storage and asserts exact watch cursor positions.
#[tokio::test]
async fn test_authz_namespace_watch_streams_snapshot_and_new_events() {
    let mut cluster = isolated_test_cluster(
        "injects namespace watch records and asserts exact cursor positions",
        &["test-region-1"],
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    append_authz_namespace_watch_record(
        &cluster.states[0].storage,
        1,
        [1; 16],
        namespace_watch_payload(10),
    )
    .await
    .unwrap();

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchAuthzNamespaceRequest {
        namespace: "document".to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let mut stream = auth_client
        .watch_authz_namespace(watch_req)
        .await
        .unwrap()
        .into_inner();

    let snapshot = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.cursor_low, 1);
    assert_eq!(snapshot.cursor_high, 0);
    assert_eq!(snapshot.namespace, "document");
    assert_eq!(snapshot.event_type, "schema_changed");
    assert_eq!(snapshot.authz_revision, 10);
    assert!(snapshot.invalidates_derived_usersets);

    append_authz_namespace_watch_record(
        &cluster.states[0].storage,
        1,
        [2; 16],
        namespace_watch_payload(11),
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.cursor_low, 2);
    assert_eq!(live.authz_revision, 11);
}

#[tokio::test]
async fn test_apply_authz_schema_persists_and_emits_namespace_watch() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-schema-apply").await;
    grant_docker_authz_realm(&cluster, &actor, "default").await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let schema = AuthzNamespaceSchema {
        namespace: "document".to_string(),
        relations: vec![
            authz_direct_relation("editor", &["user"]),
            authz_direct_relation("parent_folder", &["folder"]),
            authz_permission(
                "viewer",
                vec![
                    AuthzRelationRule {
                        kind: "inherit".to_string(),
                        relation: "editor".to_string(),
                        tuple_relation: String::new(),
                        target_relation: String::new(),
                    },
                    AuthzRelationRule {
                        kind: "computed".to_string(),
                        relation: String::new(),
                        tuple_relation: "parent_folder".to_string(),
                        target_relation: "viewer".to_string(),
                    },
                ],
            ),
        ],
        schema_json: r#"{"namespaces":{"document":{"rules":{"viewer":[{"Inherit":"editor"}]}}}}"#
            .to_string(),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    };

    let mut apply = Request::new(ApplyAuthzSchemaRequest {
        namespaces: vec![schema],
        reason: "test schema apply".to_string(),
    });
    add_bearer(&mut apply, &actor.token);
    let applied = auth_client
        .apply_authz_schema(apply)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(applied.schema_version, 1);
    assert_eq!(applied.namespaces.len(), 1);
    assert_eq!(applied.namespaces[0].namespace, "document");
    assert_eq!(applied.namespaces[0].schema_version, 1);
    assert!(!applied.namespaces[0].schema_hash.is_empty());

    let mut get_one = Request::new(GetAuthzSchemaRequest {
        namespace: "document".to_string(),
        anvil_storage_tenant_id: String::new(),
        schema_id: String::new(),
        schema_revision: None,
    });
    add_bearer(&mut get_one, &actor.token);
    let fetched = auth_client
        .get_authz_schema(get_one)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.schema_version, 1);
    assert_eq!(
        fetched.namespaces[0].schema_hash,
        applied.namespaces[0].schema_hash
    );

    let mut watch_req = Request::new(WatchAuthzNamespaceRequest {
        namespace: "document".to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    add_bearer(&mut watch_req, &actor.token);
    let mut stream = auth_client
        .watch_authz_namespace(watch_req)
        .await
        .unwrap()
        .into_inner();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.namespace, "document");
    assert_eq!(event.event_type, "schema_changed");
    assert_eq!(event.schema_hash, applied.namespaces[0].schema_hash);
    assert!(event.invalidates_derived_usersets);
}

#[tokio::test]
async fn test_authz_schema_put_bind_and_realm_scoped_tuples() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-schema-realm").await;
    let tenant_resource = format!("tenant:{}", actor.tenant_id);
    grant_docker_actor_policy(&cluster, &actor, "tenant:manage", &tenant_resource).await;
    grant_docker_actor_policy(&cluster, &actor, "authz:schema_write", "schema:default").await;
    grant_docker_actor_policy(&cluster, &actor, "authz:schema_read", "schema:default").await;
    grant_docker_authz_realm(&cluster, &actor, "realm_a").await;
    grant_docker_authz_realm(&cluster, &actor, "realm_b").await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let tenant_id = actor.tenant_id.to_string();
    let scope_a = AuthzScope {
        anvil_storage_tenant_id: tenant_id.clone(),
        authz_realm_id: "realm_a".to_string(),
    };
    let scope_b = AuthzScope {
        anvil_storage_tenant_id: tenant_id.clone(),
        authz_realm_id: "realm_b".to_string(),
    };
    let schema = AuthzNamespaceSchema {
        namespace: "document".to_string(),
        relations: vec![authz_direct_relation("viewer", &["user"])],
        schema_json: r#"{"namespaces":{"document":{"rules":{"viewer":[]}}}}"#.to_string(),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    };

    let mut put = Request::new(PutAuthzSchemaRequest {
        anvil_storage_tenant_id: tenant_id,
        schema_id: "default".to_string(),
        namespaces: vec![schema],
        reason: "realm schema test".to_string(),
    });
    add_bearer(&mut put, &actor.token);
    let schema_ref = auth_client
        .put_authz_schema(put)
        .await
        .unwrap()
        .into_inner()
        .schema_ref
        .unwrap();
    assert_eq!(schema_ref.schema_id, "default");
    assert_eq!(schema_ref.schema_revision, 1);
    assert!(!schema_ref.schema_digest.is_empty());

    for scope in [&scope_a, &scope_b] {
        let mut bind = Request::new(BindAuthzSchemaRequest {
            scope: Some(scope.clone()),
            schema_ref: Some(schema_ref.clone()),
            expected_binding_generation: None,
            reason: "bind test schema".to_string(),
        });
        add_bearer(&mut bind, &actor.token);
        let binding = auth_client
            .bind_authz_schema(bind)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(binding.binding_generation, 1);
    }

    let mut get_binding = Request::new(GetAuthzSchemaBindingRequest {
        scope: Some(scope_a.clone()),
    });
    add_bearer(&mut get_binding, &actor.token);
    let binding = auth_client
        .get_authz_schema_binding(get_binding)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        binding.schema_ref.unwrap().schema_digest,
        schema_ref.schema_digest
    );

    let mut write = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: "add".to_string(),
        reason: "realm tuple".to_string(),
        scope: Some(scope_a.clone()),
    });
    add_bearer(&mut write, &actor.token);
    auth_client.write_authz_tuple(write).await.unwrap();

    let mut check_a = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        scope: Some(scope_a),
    });
    add_bearer(&mut check_a, &actor.token);
    assert!(
        auth_client
            .check_permission(check_a)
            .await
            .unwrap()
            .into_inner()
            .allowed
    );

    let mut check_b = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        scope: Some(scope_b),
    });
    add_bearer(&mut check_b, &actor.token);
    assert!(
        !auth_client
            .check_permission(check_b)
            .await
            .unwrap()
            .into_inner()
            .allowed
    );
}

#[tokio::test]
async fn test_conditional_authz_batch_validates_bound_schema_coordinates() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-batch-schema-validation").await;
    let tenant_resource = format!("tenant:{}", actor.tenant_id);
    grant_docker_actor_policy(&cluster, &actor, "tenant:manage", &tenant_resource).await;
    grant_docker_actor_policy(&cluster, &actor, "authz:schema_write", "schema:typed").await;
    grant_docker_authz_realm(&cluster, &actor, "typed").await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let scope = AuthzScope {
        anvil_storage_tenant_id: actor.tenant_id.to_string(),
        authz_realm_id: "typed".to_string(),
    };
    let schema = vec![
        AuthzNamespaceSchema {
            namespace: "document".to_string(),
            relations: vec![
                authz_direct_relation("parent_folder", &["folder"]),
                authz_direct_relation("viewer", &["user", "userset"]),
            ],
            schema_json: r#"{"namespace":"document"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        },
        AuthzNamespaceSchema {
            namespace: "folder".to_string(),
            relations: vec![authz_direct_relation("viewer", &["user"])],
            schema_json: r#"{"namespace":"folder"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        },
    ];
    let mut put = Request::new(PutAuthzSchemaRequest {
        anvil_storage_tenant_id: actor.tenant_id.to_string(),
        schema_id: "typed".to_string(),
        namespaces: schema,
        reason: "typed batch validation".to_string(),
    });
    add_bearer(&mut put, &actor.token);
    let schema_ref = auth_client
        .put_authz_schema(put)
        .await
        .unwrap()
        .into_inner()
        .schema_ref;
    let mut bind = Request::new(BindAuthzSchemaRequest {
        scope: Some(scope.clone()),
        schema_ref,
        expected_binding_generation: None,
        reason: "bind typed schema".to_string(),
    });
    add_bearer(&mut bind, &actor.token);
    auth_client.bind_authz_schema(bind).await.unwrap();

    let make_request = |mutation: AuthzTupleMutation, operation_id: &str| {
        let mut request = Request::new(WriteAuthzTuplesRequest {
            mutations: vec![mutation],
            scope: Some(scope.clone()),
            operation_id: Some(operation_id.to_string()),
            expected_revision: None,
        });
        add_bearer(&mut request, &actor.token);
        request
    };

    let direct = auth_client
        .write_authz_tuples(make_request(
            authz_mutation("document", "doc-1", "viewer", "user", "alice", "add"),
            "valid-direct-subject",
        ))
        .await
        .unwrap()
        .into_inner();

    for (mutation, operation_id, expected_message) in [
        (
            authz_mutation("unknown", "doc-1", "viewer", "user", "alice", "add"),
            "unknown-namespace",
            "namespace unknown",
        ),
        (
            authz_mutation("document", "doc-1", "owner", "user", "alice", "add"),
            "unknown-relation",
            "relation document#owner",
        ),
        (
            authz_mutation(
                "document",
                "doc-1",
                "viewer",
                "userset",
                "folder/folder-1#owner",
                "add",
            ),
            "unknown-userset-relation",
            "subject relation folder#owner",
        ),
        (
            authz_mutation(
                "document",
                "doc-1",
                "parent_folder",
                "group",
                "group-1",
                "add",
            ),
            "unknown-edge-subject",
            "subject namespace group",
        ),
    ] {
        let error = auth_client
            .write_authz_tuples(make_request(mutation, operation_id))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains(expected_message),
            "unexpected schema validation error: {error:?}"
        );
    }

    let edge = auth_client
        .write_authz_tuples(make_request(
            authz_mutation(
                "document",
                "doc-1",
                "parent_folder",
                "folder",
                "folder-1",
                "add",
            ),
            "valid-folder-edge",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(edge.revision, direct.revision + 1);
}

fn reserved_system_realm_scope(tenant_id: i64) -> AuthzScope {
    AuthzScope {
        anvil_storage_tenant_id: tenant_id.to_string(),
        authz_realm_id: "_anvil/system".to_string(),
    }
}

fn assert_reserved_authz_status<T>(result: Result<tonic::Response<T>, tonic::Status>) {
    let err = match result {
        Ok(_) => panic!("reserved authz realm operation must fail"),
        Err(err) => err,
    };
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(
        err.message().contains("UnauthorizedReservedNamespace"),
        "expected UnauthorizedReservedNamespace, got {err:?}"
    );
}

#[tokio::test]
async fn test_public_authz_apis_reject_reserved_system_realm_scope() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "authz-reserved-realm").await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let scope = reserved_system_realm_scope(actor.tenant_id);

    let mut write = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: "add".to_string(),
        reason: "reserved realm test".to_string(),
        scope: Some(scope.clone()),
    });
    add_bearer(&mut write, &actor.token);
    assert_reserved_authz_status(auth_client.write_authz_tuple(write).await);

    let mut write_batch = Request::new(WriteAuthzTuplesRequest {
        mutations: vec![authz_mutation(
            "document", "alpha", "viewer", "user", "alice", "add",
        )],
        scope: Some(scope.clone()),
        ..Default::default()
    });
    add_bearer(&mut write_batch, &actor.token);
    assert_reserved_authz_status(auth_client.write_authz_tuples(write_batch).await);

    let mut read = Request::new(ReadAuthzTuplesRequest {
        namespace: "document".to_string(),
        object_id: String::new(),
        relation: String::new(),
        subject_kind: String::new(),
        subject_id: String::new(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        page_size: 100,
        page_token: String::new(),
        scope: Some(scope.clone()),
    });
    add_bearer(&mut read, &actor.token);
    assert_reserved_authz_status(auth_client.read_authz_tuples(read).await);

    let mut check = Request::new(CheckPermissionRequest {
        scope: Some(scope.clone()),
        ..check_permission_request("document", "alpha", "viewer", "user", "alice", "latest", "")
    });
    add_bearer(&mut check, &actor.token);
    assert_reserved_authz_status(auth_client.check_permission(check).await);

    let mut check_many = Request::new(CheckPermissionsRequest {
        checks: vec![CheckPermissionRequest {
            scope: Some(scope.clone()),
            ..check_permission_request("document", "alpha", "viewer", "user", "alice", "latest", "")
        }],
    });
    add_bearer(&mut check_many, &actor.token);
    assert_reserved_authz_status(auth_client.check_permissions(check_many).await);

    let mut list_objects = Request::new(ListAuthzObjectsRequest {
        namespace: "document".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        page_size: 100,
        page_token: String::new(),
        scope: Some(scope.clone()),
    });
    add_bearer(&mut list_objects, &actor.token);
    assert_reserved_authz_status(auth_client.list_authz_objects(list_objects).await);

    let mut list_subjects = Request::new(ListAuthzSubjectsRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
        page_size: 100,
        page_token: String::new(),
        scope: Some(scope.clone()),
    });
    add_bearer(&mut list_subjects, &actor.token);
    assert_reserved_authz_status(auth_client.list_authz_subjects(list_subjects).await);

    let mut bind = Request::new(BindAuthzSchemaRequest {
        scope: Some(scope.clone()),
        schema_ref: None,
        expected_binding_generation: None,
        reason: "reserved realm test".to_string(),
    });
    add_bearer(&mut bind, &actor.token);
    assert_reserved_authz_status(auth_client.bind_authz_schema(bind).await);

    let mut get_binding = Request::new(GetAuthzSchemaBindingRequest {
        scope: Some(scope.clone()),
    });
    add_bearer(&mut get_binding, &actor.token);
    assert_reserved_authz_status(auth_client.get_authz_schema_binding(get_binding).await);

    let mut watch = Request::new(WatchAuthzTupleLogRequest {
        after_revision: 0,
        namespace: "document".to_string(),
        scope: Some(scope),
    });
    add_bearer(&mut watch, &actor.token);
    assert_reserved_authz_status(auth_client.watch_authz_tuple_log(watch).await);
}

// This test stays in-process because it injects derived-lag watch records
// through cluster.states.storage and asserts exact watch cursor positions.
#[tokio::test]
async fn test_authz_derived_lag_watch_streams_snapshot_and_new_events() {
    let mut cluster = isolated_test_cluster(
        "injects derived lag watch records and asserts exact cursor positions",
        &["test-region-1"],
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    append_authz_derived_lag_watch_record(
        &cluster.states[0].storage,
        1,
        [1; 16],
        derived_lag_watch_payload(90, 100, 1),
        &[],
    )
    .await
    .unwrap();

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchAuthzDerivedLagRequest {
        derived_index_id: "derived-userset-primary".to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let mut stream = auth_client
        .watch_authz_derived_lag(watch_req)
        .await
        .unwrap()
        .into_inner();

    let snapshot = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.cursor_low, 1);
    assert_eq!(snapshot.derived_index_id, "derived-userset-primary");
    assert_eq!(snapshot.derived_index_kind, "userset");
    assert_eq!(snapshot.processed_revision, 90);
    assert_eq!(snapshot.latest_revision, 100);
    assert_eq!(snapshot.revision_lag, 10);
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.authz_revision, 100);

    append_authz_derived_lag_watch_record(
        &cluster.states[0].storage,
        1,
        [2; 16],
        derived_lag_watch_payload(100, 100, 2),
        &[],
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.cursor_low, 2);
    assert_eq!(live.revision_lag, 0);
    assert_eq!(live.generation, 2);
}

// This test stays in-process because it opens the local CoreMetaStore path and
// deletes a derived-userset row to force repair.
#[tokio::test]
async fn test_repair_authz_derived_index_rebuilds_from_tuple_log() {
    let mut cluster = isolated_test_cluster(
        "deletes the global derived-userset index row and asserts exact repair revisions",
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
    let mut repair_client = RepairServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut direct_grant = Request::new(write_authz_tuple_request(
        "document",
        "repair-alpha",
        "viewer",
        "user",
        "alice",
        "add",
    ));
    add_bearer(&mut direct_grant, &token);
    auth_client.write_authz_tuple(direct_grant).await.unwrap();

    let derived_index_row_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("derived_userset_index"),
        CoreMetaTuplePart::I64(1),
        CoreMetaTuplePart::Utf8(DEFAULT_DERIVED_USERSET_INDEX_ID),
    ])
    .unwrap();
    let meta = CoreMetaStore::open(cluster.states[0].storage.core_store_meta_path()).unwrap();
    assert!(
        meta.get(CF_AUTHZ, TABLE_AUTHZ_TUPLE_PAGE_ROW, &derived_index_row_key,)
            .unwrap()
            .is_some(),
        "derived userset CoreMeta row must exist before repair test"
    );
    meta.delete(CF_AUTHZ, TABLE_AUTHZ_TUPLE_PAGE_ROW, &derived_index_row_key)
        .expect("delete derived userset CoreMeta row to force repair");

    let mut repair_request = Request::new(RepairAuthzDerivedIndexRequest {
        derived_index_id: DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
        rebuild: true,
    });
    add_bearer(&mut repair_request, &token);
    let repaired = repair_client
        .repair_authz_derived_index(repair_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repaired.status, "rebuilt_derived_index");
    assert_eq!(repaired.reason, "AuthzDerivedIndexMissing");
    assert_eq!(repaired.latest_revision, 1);
    assert_eq!(repaired.processed_revision, 1);
    assert_eq!(
        repaired.derived_index_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID.to_string()
    );
    assert!(repaired.finding.is_some());

    let mut check = Request::new(check_permission_request(
        "document",
        "repair-alpha",
        "viewer",
        "user",
        "alice",
        "exact",
        "authz:1",
    ));
    add_bearer(&mut check, &token);
    let allowed = auth_client
        .check_permission(check)
        .await
        .unwrap()
        .into_inner();
    assert!(allowed.allowed);

    let mut list_findings = Request::new(ListRepairFindingsRequest {
        scope_kind: "authz".to_string(),
        scope_id: "tenant-1".to_string(),
        page: Some(anvil::anvil_api::PageRequest {
            page_size: 10,
            page_token: String::new(),
        }),
    });
    add_bearer(&mut list_findings, &token);
    let findings = repair_client
        .list_repair_findings(list_findings)
        .await
        .unwrap()
        .into_inner()
        .findings;
    assert!(findings.iter().any(|finding| {
        finding.code == "AuthzDerivedIndexMissing"
            && finding.proposed_action == "RebuildDerivedIndex"
    }));

    let mut second_repair = Request::new(RepairAuthzDerivedIndexRequest {
        derived_index_id: DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
        rebuild: false,
    });
    add_bearer(&mut second_repair, &token);
    let up_to_date = repair_client
        .repair_authz_derived_index(second_repair)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(up_to_date.status, "up_to_date");
    assert!(up_to_date.finding.is_none());
}

fn namespace_watch_payload(authz_revision: u64) -> AuthzNamespaceWatchPayload {
    AuthzNamespaceWatchPayload {
        namespace: "document".to_string(),
        event_type: "schema_changed".to_string(),
        authz_revision,
        schema_hash: hex::encode([4; 32]),
        invalidates_derived_usersets: true,
        emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }
}

fn derived_lag_watch_payload(
    processed_revision: u64,
    latest_revision: u64,
    generation: u64,
) -> AuthzDerivedLagWatchPayload {
    AuthzDerivedLagWatchPayload {
        derived_index_id: "derived-userset-primary".to_string(),
        derived_index_kind: "userset".to_string(),
        processed_revision,
        latest_revision,
        source_cursor: u128::from(latest_revision),
        source_manifest_hash: hex::encode([9; 32]),
        generation,
        emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }
}
