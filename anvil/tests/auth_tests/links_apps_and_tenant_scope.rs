use super::*;

#[tokio::test]
async fn tenant_can_create_update_delete_object_link() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "tenant-object-link").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("tenant-link");
    let target_v1 = "releases/app-v1.exe".to_string();
    let target_v2 = "releases/app-v2.exe".to_string();
    let link_key = "releases/latest.exe".to_string();
    let hidden_link_key = "releases/internal.exe".to_string();

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
        &target_v1,
        b"v1",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        actor.tenant_id,
        bucket_id,
        &actor.app_id,
        &bucket_name,
        &target_v2,
        b"v2",
    )
    .await;

    let mut create_link = Request::new(CreateObjectLinkRequest {
        context: Some(public_mutation_context("link-create", 0)),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        link_key: link_key.clone(),
        target_key: target_v1.clone(),
        target_version: String::new(),
        resolution: anvil::anvil_api::ObjectLinkResolution::Follow as i32,
        allow_dangling: false,
    });
    add_bearer(&mut create_link, &token);
    let created = object_client
        .create_object_link(create_link)
        .await
        .unwrap()
        .into_inner()
        .link
        .expect("created link");
    assert_eq!(created.link_key, link_key);
    assert_eq!(created.target_key, target_v1);

    let mut read_link = Request::new(ReadObjectLinkRequest {
        request_id: "read-created-link".to_string(),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        link_key: link_key.clone(),

        ..Default::default()
    });
    add_bearer(&mut read_link, &token);
    let read = object_client
        .read_object_link(read_link)
        .await
        .unwrap()
        .into_inner()
        .link
        .expect("read link");
    assert_eq!(read.target_key, target_v1);

    let mut create_hidden = Request::new(CreateObjectLinkRequest {
        context: Some(public_mutation_context("link-create-hidden", 0)),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        link_key: hidden_link_key.clone(),
        target_key: target_v1.clone(),
        target_version: String::new(),
        resolution: anvil::anvil_api::ObjectLinkResolution::Follow as i32,
        allow_dangling: false,
    });
    add_bearer(&mut create_hidden, &token);
    object_client
        .create_object_link(create_hidden)
        .await
        .unwrap();

    let link_resource = format!("{bucket_name}/{link_key}");
    let limited = cluster
        .create_actor_in_tenant(
            actor.tenant_id,
            "limited-link",
            &[
                ("object:list", bucket_name.as_str()),
                ("object:read", link_resource.as_str()),
            ],
        )
        .await;
    let limited_token = limited.token.clone();
    let mut filtered_list = Request::new(ListObjectLinksRequest {
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        prefix: "releases/".to_string(),
        page: None,

        ..Default::default()
    });
    add_bearer(&mut filtered_list, &limited_token);
    let filtered = object_client
        .list_object_links(filtered_list)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(filtered.links.len(), 1);
    assert_eq!(filtered.links[0].link_key, link_key);

    let mut update_link = Request::new(UpdateObjectLinkRequest {
        context: Some(public_mutation_context("link-update", created.generation)),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        link_key: link_key.clone(),
        target_key: target_v2.clone(),
        target_version: String::new(),
        resolution: anvil::anvil_api::ObjectLinkResolution::Follow as i32,
        allow_dangling: false,
    });
    add_bearer(&mut update_link, &token);
    let updated = object_client
        .update_object_link(update_link)
        .await
        .unwrap()
        .into_inner()
        .link
        .expect("updated link");
    assert_eq!(updated.target_key, target_v2);

    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("link-reader-tenant"))
        .await;
    let tenant_b = cluster
        .create_actor_in_tenant(tenant_b_id, "tenant-link-reader", &[])
        .await;
    let mut denied_read = Request::new(ReadObjectLinkRequest {
        request_id: "tenant-b-read-link".to_string(),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        link_key: link_key.clone(),

        ..Default::default()
    });
    add_bearer(&mut denied_read, &tenant_b.token);
    let denied = object_client
        .read_object_link(denied_read)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::NotFound);

    let mut delete_link = Request::new(DeleteObjectLinkRequest {
        context: Some(public_mutation_context("link-delete", updated.generation)),
        tenant_id: String::new(),
        bucket_name,
        link_key: link_key.clone(),
    });
    add_bearer(&mut delete_link, &token);
    let deleted = object_client
        .delete_object_link(delete_link)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.resource_id, link_key);
    assert_eq!(deleted.generation, updated.generation + 1);
}

#[tokio::test]
async fn tenant_can_request_and_verify_host_alias() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "tenant-host-alias").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("host-alias");
    let hostname = format!("{}.example.com", uuid::Uuid::new_v4().simple());

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
    bucket_client.create_bucket(create_bucket).await.unwrap();

    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("host-alias-tenant"))
        .await;
    let tenant_b = cluster
        .create_actor_in_tenant(tenant_b_id, "tenant-host-alias", &[])
        .await;

    let mut denied_create = Request::new(CreateHostAliasRequest {
        context: Some(public_mutation_context("host-alias-denied-create", 0)),
        hostname: hostname.clone(),
        tenant_id: tenant_b_id.to_string(),
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
        prefix: "public/".to_string(),
    });
    add_bearer(&mut denied_create, &tenant_b.token);
    let denied = object_client
        .create_host_alias(denied_create)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::NotFound);

    let mut create_alias = Request::new(CreateHostAliasRequest {
        context: Some(public_mutation_context("host-alias-create", 0)),
        hostname: hostname.clone(),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
        prefix: "public/".to_string(),
    });
    add_bearer(&mut create_alias, &token);
    let created = object_client
        .create_host_alias(create_alias)
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .expect("host alias descriptor");
    assert_eq!(created.hostname, hostname);
    assert_eq!(created.tenant_id, actor.tenant_id.to_string());
    assert_eq!(created.bucket_name, bucket_name);
    assert_eq!(created.state, 1);
    assert!(
        created
            .verification_challenge
            .starts_with("anvil-host-alias=")
    );

    let mut denied_read = Request::new(ReadHostAliasRequest {
        request_id: "host-alias-denied-read".to_string(),
        hostname: hostname.clone(),
    });
    add_bearer(&mut denied_read, &tenant_b.token);
    let denied = object_client
        .read_host_alias(denied_read)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::NotFound);

    let mut read_alias = Request::new(ReadHostAliasRequest {
        request_id: "host-alias-read".to_string(),
        hostname: hostname.clone(),
    });
    add_bearer(&mut read_alias, &token);
    let read = object_client
        .read_host_alias(read_alias)
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .expect("host alias descriptor");
    assert_eq!(read.hostname, hostname);
    assert_eq!(read.verification_challenge, created.verification_challenge);

    let mut wrong_verify = Request::new(VerifyHostAliasRequest {
        context: Some(public_mutation_context(
            "host-alias-wrong-verify",
            created.generation,
        )),
        hostname: hostname.clone(),
        observed_challenge: "anvil-host-alias=wrong".to_string(),
    });
    add_bearer(&mut wrong_verify, &token);
    let wrong = object_client
        .verify_host_alias(wrong_verify)
        .await
        .unwrap_err();
    assert_eq!(wrong.code(), tonic::Code::FailedPrecondition);

    let mut verify_alias = Request::new(VerifyHostAliasRequest {
        context: Some(public_mutation_context(
            "host-alias-verify",
            created.generation,
        )),
        hostname: hostname.clone(),
        observed_challenge: created.verification_challenge.clone(),
    });
    add_bearer(&mut verify_alias, &token);
    let verified = object_client
        .verify_host_alias(verify_alias)
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .expect("verified host alias descriptor");
    assert_eq!(verified.hostname, hostname);
    assert_eq!(verified.state, 2);

    let mut list_aliases = Request::new(ListHostAliasesRequest {
        region: "test-region-1".to_string(),
        page: None,
    });
    add_bearer(&mut list_aliases, &token);
    let listed = object_client
        .list_host_aliases(list_aliases)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.host_aliases.len(), 1);
    assert_eq!(listed.host_aliases[0].hostname, hostname);

    let mut delete_alias = Request::new(DeleteHostAliasRequest {
        context: Some(public_mutation_context(
            "host-alias-delete",
            verified.generation,
        )),
        hostname: hostname.clone(),
    });
    add_bearer(&mut delete_alias, &token);
    let deleted = object_client
        .delete_host_alias(delete_alias)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.resource_id, hostname);
    assert_eq!(deleted.generation, verified.generation + 1);
}

#[tokio::test]
async fn tenant_object_link_cannot_cross_tenant_without_delegation() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "cross-tenant-link").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("cross-link");
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
    bucket_client.create_bucket(create_bucket).await.unwrap();

    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("link-writer-tenant"))
        .await;
    let tenant_b = cluster
        .create_actor_in_tenant(tenant_b_id, "tenant-link-writer", &[])
        .await;
    let mut create_link = Request::new(CreateObjectLinkRequest {
        context: Some(public_mutation_context("cross-tenant-link-create", 0)),
        tenant_id: String::new(),
        bucket_name: bucket_name.clone(),
        link_key: "latest".to_string(),
        target_key: "target".to_string(),
        target_version: String::new(),
        resolution: anvil::anvil_api::ObjectLinkResolution::Follow as i32,
        allow_dangling: true,
    });
    add_bearer(&mut create_link, &tenant_b.token);
    let denied = object_client
        .create_object_link(create_link)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn tenant_link_list_is_authz_filtered() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "link-list-filtered").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("filtered-link");
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
    bucket_client.create_bucket(create_bucket).await.unwrap();

    for key in ["visible", "hidden"] {
        let mut create_link = Request::new(CreateObjectLinkRequest {
            context: Some(public_mutation_context(&format!("filtered-link-{key}"), 0)),
            tenant_id: String::new(),
            bucket_name: bucket_name.clone(),
            link_key: format!("releases/{key}"),
            target_key: format!("targets/{key}"),
            target_version: String::new(),
            resolution: anvil::anvil_api::ObjectLinkResolution::Follow as i32,
            allow_dangling: true,
        });
        add_bearer(&mut create_link, &token);
        object_client.create_object_link(create_link).await.unwrap();
    }

    let visible_resource = format!("{bucket_name}/releases/visible");
    let limited = cluster
        .create_actor_in_tenant(
            actor.tenant_id,
            "limited-lister",
            &[
                ("object:list", bucket_name.as_str()),
                ("object:read", visible_resource.as_str()),
            ],
        )
        .await;
    let limited_token = limited.token.clone();
    let mut list = Request::new(ListObjectLinksRequest {
        tenant_id: String::new(),
        bucket_name,
        prefix: "releases/".to_string(),
        page: None,

        ..Default::default()
    });
    add_bearer(&mut list, &limited_token);
    let listed = object_client
        .list_object_links(list)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.links.len(), 1);
    assert_eq!(listed.links[0].link_key, "releases/visible");
}

#[tokio::test]
async fn tenant_can_create_and_rotate_own_app_secret() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "tenant-app-secret").await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let app_name = unique_test_name("tenant-managed");
    let mut create = Request::new(CreateApplicationCredentialRequest {
        app_name: app_name.clone(),
        request_id: format!("create-{app_name}"),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
    });
    add_bearer(&mut create, &token);
    let created = auth_client
        .create_application_credential(create)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.tenant_id, actor.tenant_id.to_string());
    assert_eq!(created.app_name, app_name);
    assert!(!created.client_secret.is_empty());

    let mut grant = Request::new(GrantAccessRequest {
        grantee_app_id: app_name.clone(),
        resource: "buckets".to_string(),
        action: "bucket:list".to_string(),
    });
    add_bearer(&mut grant, &token);
    auth_client.grant_access(grant).await.unwrap();

    let token_before_rotate =
        get_token(&actor.grpc_addr, &created.client_id, &created.client_secret).await;
    assert!(!token_before_rotate.is_empty());

    let mut rotate = Request::new(RotateApplicationCredentialSecretRequest {
        app_name: app_name.clone(),
        request_id: format!("rotate-{app_name}"),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
    });
    add_bearer(&mut rotate, &token);
    let rotated = auth_client
        .rotate_application_credential_secret(rotate)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rotated.client_id, created.client_id);
    assert_ne!(rotated.client_secret, created.client_secret);

    let old_secret =
        try_get_token(&actor.grpc_addr, &created.client_id, &created.client_secret).await;
    assert!(old_secret.is_err());
    let new_secret = get_token(&actor.grpc_addr, &rotated.client_id, &rotated.client_secret).await;
    assert!(!new_secret.is_empty());

    let mut list = Request::new(ListApplicationsRequest { page: None });
    add_bearer(&mut list, &token);
    let apps = auth_client
        .list_applications(list)
        .await
        .unwrap()
        .into_inner();
    assert!(apps.applications.iter().any(|app| app.app_name == app_name));

    let mut delete = Request::new(DeleteApplicationCredentialRequest {
        app_name,
        request_id: "delete-tenant-managed-app".to_string(),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
    });
    add_bearer(&mut delete, &token);
    auth_client
        .delete_application_credential(delete)
        .await
        .unwrap();
}

#[tokio::test]
async fn tenant_cannot_manage_other_tenant_app_secret() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "other-app-secret").await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let tenant_one_app = unique_test_name("tenant-one-app");
    let mut create = Request::new(CreateApplicationCredentialRequest {
        app_name: tenant_one_app.clone(),
        request_id: format!("create-{tenant_one_app}"),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
    });
    add_bearer(&mut create, &token);
    auth_client
        .create_application_credential(create)
        .await
        .unwrap();

    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("app-manager-tenant"))
        .await;
    let tenant_b_resource = format!("tenant:{tenant_b_id}");
    let tenant_b = cluster
        .create_actor_in_tenant(
            tenant_b_id,
            "tenant-app-manager",
            &[("app:rotate_secret", tenant_b_resource.as_str())],
        )
        .await;
    let mut rotate = Request::new(RotateApplicationCredentialSecretRequest {
        app_name: tenant_one_app,
        request_id: "rotate-other-tenant-app".to_string(),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
    });
    add_bearer(&mut rotate, &tenant_b.token);
    let denied = auth_client
        .rotate_application_credential_secret(rotate)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn tenant_can_delegate_narrower_policy_capability() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "delegate-policy").await;

    let grantee = cluster
        .create_actor_in_tenant(actor.tenant_id, "narrow-grantee", &[])
        .await;
    let grantee_app = grantee.app_name.clone();
    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut grant = Request::new(GrantAccessRequest {
        grantee_app_id: grantee_app.clone(),
        resource: "buckets".to_string(),
        action: "bucket:list".to_string(),
    });
    add_bearer(&mut grant, &token);
    auth_client.grant_access(grant).await.unwrap();

    let mut list = Request::new(ListAccessGrantsRequest {
        app: grantee_app,
        page: None,
    });
    add_bearer(&mut list, &token);
    let grants = auth_client
        .list_access_grants(list)
        .await
        .unwrap()
        .into_inner();
    assert!(
        grants
            .grants
            .iter()
            .any(|grant| { grant.action == "bucket:list" })
    );

    assert!(!grantee.token.is_empty());
}

#[tokio::test]
async fn tenant_cannot_grant_system_realm_or_cross_tenant_authority() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "blocked-grants").await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let blocked_app = cluster
        .create_actor_in_tenant(actor.tenant_id, "blocked-grantee", &[])
        .await
        .app_name;
    let foreign_tenant_id = cluster
        .create_tenant(&unique_test_name("blocked-foreign"))
        .await;
    let cross_bucket = format!("tenant:{foreign_tenant_id}/bucket:foreign");
    let cross_authz = format!("tenant-{foreign_tenant_id}/authz/default");

    for resource in [
        "system:mesh/default",
        cross_bucket.as_str(),
        cross_authz.as_str(),
    ] {
        let mut grant = Request::new(GrantAccessRequest {
            grantee_app_id: blocked_app.clone(),
            resource: resource.to_string(),
            action: "bucket:read".to_string(),
        });
        add_bearer(&mut grant, &token);
        let denied = auth_client.grant_access(grant).await.unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{resource}");
    }
}

#[tokio::test]
async fn tenant_cannot_bind_host_alias_to_other_tenant_bucket() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "host-alias-cross").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("alias-cross");
    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    add_bearer(&mut create_bucket, &token);
    bucket_client.create_bucket(create_bucket).await.unwrap();

    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("alias-cross-tenant"))
        .await;
    let tenant_b = cluster
        .create_actor_in_tenant(tenant_b_id, "tenant-host-alias", &[])
        .await;
    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create_alias = Request::new(CreateHostAliasRequest {
        context: Some(public_mutation_context("host-alias-cross-tenant", 0)),
        hostname: format!("{}.example.com", uuid::Uuid::new_v4().simple()),
        tenant_id: String::new(),
        bucket_name,
        region: "test-region-1".to_string(),
        prefix: String::new(),
    });
    add_bearer(&mut create_alias, &tenant_b.token);
    let denied = object_client
        .create_host_alias(create_alias)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn tenant_diagnostics_are_tenant_scoped() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "tenant-diagnostics").await;

    let token = actor.token.clone();
    let bucket_name = unique_test_name("diagnostics");
    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),
        options: None,
    });
    add_bearer(&mut create_bucket, &token);
    bucket_client.create_bucket(create_bucket).await.unwrap();

    let mut index_client = IndexServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut allowed = Request::new(ListIndexDiagnosticsRequest {
        bucket_name: bucket_name.clone(),
        index_name: String::new(),
        severity: String::new(),
        page: Some(anvil::anvil_api::PageRequest {
            page_size: 10,
            page_token: String::new(),
        }),
    });
    add_bearer(&mut allowed, &token);
    index_client.list_index_diagnostics(allowed).await.unwrap();

    let denied_actor = cluster
        .create_actor_in_tenant(actor.tenant_id, "diagnostics-denied", &[])
        .await;
    let mut denied = Request::new(ListIndexDiagnosticsRequest {
        bucket_name,
        index_name: String::new(),
        severity: String::new(),
        page: Some(anvil::anvil_api::PageRequest {
            page_size: 10,
            page_token: String::new(),
        }),
    });
    add_bearer(&mut denied, &denied_actor.token);
    let err = index_client
        .list_index_diagnostics(denied)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn tenant_repair_cannot_target_other_tenant() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "tenant-repair").await;

    let bucket_name = unique_test_name("tenant-repair");
    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),
        options: None,
    });
    add_bearer(&mut create_bucket, &actor.token);
    bucket_client.create_bucket(create_bucket).await.unwrap();
    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("repair-tenant"))
        .await;
    let tenant_b = cluster
        .create_actor_in_tenant(tenant_b_id, "tenant-repair", &[])
        .await;
    let mut repair_client = RepairServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut request = Request::new(RepairIndexRequest {
        bucket_name,
        index_name: "search".to_string(),
        rebuild: false,
    });
    add_bearer(&mut request, &tenant_b.token);
    let err = repair_client.repair_index(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn tenant_audit_page_token_cannot_be_reused_by_other_tenant() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "tenant-audit-page").await;

    let token = actor.token.clone();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let audit_apps = [
        unique_test_name("audit-app-a"),
        unique_test_name("audit-app-b"),
    ];
    for app_name in &audit_apps {
        let mut create = Request::new(CreateApplicationCredentialRequest {
            app_name: app_name.clone(),
            request_id: format!("create-{app_name}"),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
        });
        add_bearer(&mut create, &token);
        auth_client
            .create_application_credential(create)
            .await
            .unwrap();
    }

    let mut audit_client = AuditServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut first_page = Request::new(ListAuditEventsRequest {
        request_id: "tenant-a-audit-first-page".to_string(),
        principal_id: String::new(),
        resource_id: String::new(),
        action: String::new(),
        page: Some(PageRequest {
            page_token: String::new(),
            page_size: 1,
        }),
    });
    add_bearer(&mut first_page, &token);
    let first_page = audit_client
        .list_tenant_audit_events(first_page)
        .await
        .unwrap()
        .into_inner();
    let cursor = first_page.page.unwrap().next_page_token;
    assert!(!cursor.is_empty());

    let tenant_resource = format!("tenant:{}", actor.tenant_id);
    let reader = cluster
        .create_actor_in_tenant(
            actor.tenant_id,
            "tenant-a-audit",
            &[("app:read", tenant_resource.as_str())],
        )
        .await;
    let mut reused = Request::new(ListAuditEventsRequest {
        request_id: "tenant-a-audit-reuse-different-principal".to_string(),
        principal_id: String::new(),
        resource_id: String::new(),
        action: String::new(),
        page: Some(PageRequest {
            page_token: cursor.clone(),
            page_size: 1,
        }),
    });
    add_bearer(&mut reused, &reader.token);
    let err = audit_client
        .list_tenant_audit_events(reused)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let tenant_b_id = cluster
        .create_tenant(&unique_test_name("audit-tenant"))
        .await;
    let tenant_b = cluster
        .create_actor_in_tenant(tenant_b_id, "tenant-b-audit", &[])
        .await;
    let mut cross_tenant = Request::new(ListAuditEventsRequest {
        request_id: "tenant-b-audit-reuse".to_string(),
        principal_id: String::new(),
        resource_id: String::new(),
        action: String::new(),
        page: Some(PageRequest {
            page_token: cursor,
            page_size: 1,
        }),
    });
    add_bearer(&mut cross_tenant, &tenant_b.token);
    let err = audit_client
        .list_tenant_audit_events(cross_tenant)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
