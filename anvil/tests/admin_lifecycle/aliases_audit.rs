use super::*;

#[tokio::test]
async fn admin_host_aliases_are_generation_checked_and_lifecycle_managed() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let denied_token = non_admin_token(&node);

    let mut public_client = AdminServiceClient::connect(node.public_url.clone())
        .await
        .unwrap();
    let public_err = public_client
        .list_host_aliases(with_auth(
            tonic::Request::new(ListHostAliasesRequest {
                region: String::new(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(public_err.code(), Code::Unimplemented);

    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let denied = client
        .list_host_aliases(with_auth(
            tonic::Request::new(ListHostAliasesRequest {
                region: String::new(),
                page: None,
            }),
            &denied_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let region = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("alias-create-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: String::new(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    prepare_active_region_dependencies(
        &mut client,
        &token,
        "alias",
        "eu-west-1",
        "cell-a",
        "node-a",
    )
    .await;
    let _region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("alias-activate-region", region.generation)),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: activation_checkpoint_json_from_existing_streams(
                    &node,
                    "eu-west-1",
                )
                .await,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();

    let native_hostname = client
        .create_host_alias(with_auth(
            tonic::Request::new(CreateHostAliasAdminRequest {
                context: Some(context("alias-native-hostname", 0)),
                hostname: "releases.tenant-alias.eu-west-1.anvil-storage.test".to_string(),
                tenant_id: "tenant-alias".to_string(),
                bucket_name: "releases".to_string(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(native_hostname.code(), Code::InvalidArgument);

    let created = client
        .create_host_alias(with_auth(
            tonic::Request::new(CreateHostAliasAdminRequest {
                context: Some(context("alias-create", 0)),
                hostname: "CDN.Example.Com.".to_string(),
                tenant_id: "tenant-alias".to_string(),
                bucket_name: "releases".to_string(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(created.hostname, "cdn.example.com");
    assert_eq!(created.state, 1);
    assert_eq!(created.generation, 1);

    let stale = client
        .activate_host_alias(with_auth(
            tonic::Request::new(ActivateHostAliasRequest {
                context: Some(context("alias-activate-stale", created.generation + 1)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Aborted);

    let missing_generation = client
        .activate_host_alias(with_auth(
            tonic::Request::new(ActivateHostAliasRequest {
                context: Some(context("alias-activate-missing-generation", 0)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(missing_generation.code(), Code::InvalidArgument);

    let active = client
        .activate_host_alias(with_auth(
            tonic::Request::new(ActivateHostAliasRequest {
                context: Some(context("alias-activate", created.generation)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(active.state, 2);
    assert_eq!(active.generation, created.generation + 1);

    let read = client
        .read_host_alias(with_auth(
            tonic::Request::new(ReadHostAliasRequest {
                request_id: "req-alias-read".to_string(),
                hostname: "CDN.EXAMPLE.COM.".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(read.hostname, "cdn.example.com");
    assert_eq!(read.state, 2);
    assert_eq!(read.generation, active.generation);

    let routing_records = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 4,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    let host_alias_record = routing_records
        .iter()
        .find(|record| record.record_key == "cdn.example.com")
        .expect("host alias routing record should be materialised");
    assert_eq!(host_alias_record.family, 4);
    assert_eq!(host_alias_record.generation, active.generation);
    assert!(
        host_alias_record
            .payload_json
            .contains("\"cdn.example.com\"")
    );

    let listed = client
        .list_host_aliases(with_auth(
            tonic::Request::new(ListHostAliasesRequest {
                region: "eu-west-1".to_string(),
                page: Some(PageRequest {
                    page_token: String::new(),
                    page_size: 10,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.host_aliases.len(), 1);
    assert_eq!(listed.host_aliases[0].hostname, "cdn.example.com");
    assert_eq!(listed.host_aliases[0].state, 2);

    let suspended = client
        .suspend_host_alias(with_auth(
            tonic::Request::new(SuspendHostAliasRequest {
                context: Some(context("alias-suspend", active.generation)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(suspended.state, 3);

    let deleted = client
        .delete_host_alias(with_auth(
            tonic::Request::new(DeleteHostAliasAdminRequest {
                context: Some(context("alias-delete", suspended.generation)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.resource_id, "cdn.example.com");
    assert_eq!(deleted.generation, suspended.generation + 1);
}

#[tokio::test]
async fn admin_mutations_are_returned_by_durable_audit_listing() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant_response = client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("audit-create-tenant", 0)),
                name: "audit-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let tenant = tenant_response.tenant.clone().unwrap();
    let tenant_id = tenant.tenant_id.parse::<i64>().unwrap();

    let app_response = client
        .create_application(with_auth(
            tonic::Request::new(CreateApplicationRequest {
                context: Some(context("audit-create-app", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "publisher".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let bucket_response = client
        .create_bucket_admin(with_auth(
            tonic::Request::new(CreateBucketAdminRequest {
                context: Some(context("audit-create-bucket", 0)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: "release-assets".to_string(),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let bucket = bucket_response.bucket.clone().unwrap();

    let public_bucket_response = client
        .set_bucket_public_access_admin(with_auth(
            tonic::Request::new(SetBucketPublicAccessAdminRequest {
                context: Some(context("audit-public-bucket", 1)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: bucket.name.clone(),
                allow_public_read: true,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let region_response = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("audit-create-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: String::new(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let region = region_response.region.clone().unwrap();
    prepare_active_region_dependencies(
        &mut client,
        &token,
        "audit",
        "eu-west-1",
        "cell-a",
        "node-a",
    )
    .await;
    let active_region_response = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("audit-activate-region", region.generation)),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: activation_checkpoint_json_from_existing_streams(
                    &node,
                    "eu-west-1",
                )
                .await,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let host_alias_response = client
        .create_host_alias(with_auth(
            tonic::Request::new(CreateHostAliasAdminRequest {
                context: Some(context("audit-create-host-alias", 0)),
                hostname: "Audit.Example.Com.".to_string(),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: bucket.name.clone(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let audit = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "req-list-admin-audit".to_string(),
                principal_id: "admin-principal".to_string(),
                resource_id: String::new(),
                action: String::new(),
                page: Some(PageRequest {
                    page_token: String::new(),
                    page_size: 100,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let first_page = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "req-list-admin-audit-page-1".to_string(),
                principal_id: "admin-principal".to_string(),
                resource_id: String::new(),
                action: String::new(),
                page: Some(PageRequest {
                    page_token: String::new(),
                    page_size: 2,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.events.len(), 2);
    let first_page_cursor = first_page.page.unwrap().next_page_token;
    assert!(!first_page_cursor.is_empty());

    let second_page = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "req-list-admin-audit-page-2".to_string(),
                principal_id: "admin-principal".to_string(),
                resource_id: String::new(),
                action: String::new(),
                page: Some(PageRequest {
                    page_token: first_page_cursor.clone(),
                    page_size: 2,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.events.len(), 2);
    assert_ne!(
        first_page.events[0].audit_event_id,
        second_page.events[0].audit_event_id
    );

    let filter_mismatch = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "req-list-admin-audit-page-filter-mismatch".to_string(),
                principal_id: "admin-principal".to_string(),
                resource_id: String::new(),
                action: "admin.tenant.create".to_string(),
                page: Some(PageRequest {
                    page_token: first_page_cursor.clone(),
                    page_size: 2,
                }),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(filter_mismatch.code(), Code::InvalidArgument);

    client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("audit-cursor-stale-create-tenant", 0)),
                name: "audit-cursor-stale-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap();
    let stale_revision = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "req-list-admin-audit-page-stale-revision".to_string(),
                principal_id: "admin-principal".to_string(),
                resource_id: String::new(),
                action: String::new(),
                page: Some(PageRequest {
                    page_token: first_page_cursor,
                    page_size: 2,
                }),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_revision.code(), Code::InvalidArgument);

    let find_event = |action: &str| {
        audit
            .events
            .iter()
            .find(|event| event.action == action)
            .unwrap_or_else(|| panic!("missing audit event for {action}"))
    };
    let details = |event: &AuditEventRecord| -> serde_json::Value {
        serde_json::from_str(&event.details_json).unwrap()
    };

    let tenant_event = find_event("admin.tenant.create");
    assert_eq!(tenant_event.audit_event_id, tenant_response.audit_event_id);
    assert_eq!(tenant_event.resource_id, format!("tenant:{tenant_id}"));
    assert_eq!(tenant_event.principal_id, "admin-principal");
    let tenant_details = details(tenant_event);
    assert_eq!(tenant_details["tenant_name"], "audit-tenant");
    assert_eq!(
        tenant_details["idempotency_key"],
        "idem-audit-create-tenant"
    );
    assert_eq!(tenant_details["authorised_relation"], "manage_tenants");
    assert_eq!(
        tenant_details["authorised_object"],
        serde_json::json!(format!(
            "{}:{}",
            anvil::system_realm::system_namespace(),
            anvil::system_realm::system_mesh_object_id("mesh-test")
        ))
    );

    let app_event = find_event("admin.app.create");
    assert_eq!(app_event.audit_event_id, app_response.audit_event_id);
    let app_details = details(app_event);
    assert_eq!(app_details["tenant_id"], tenant_id);
    assert_eq!(app_details["app_name"], "publisher");
    assert_eq!(app_details["authorised_relation"], "manage_apps");

    let bucket_event = find_event("admin.bucket.create");
    assert_eq!(bucket_event.audit_event_id, bucket_response.audit_event_id);
    assert_eq!(
        bucket_event.resource_id,
        format!("tenant:{tenant_id}:bucket:release-assets")
    );
    let bucket_details = details(bucket_event);
    assert_eq!(bucket_details["bucket_id"], bucket.bucket_id);
    assert_eq!(bucket_details["region"], "eu-west-1");

    let public_bucket_event = find_event("admin.bucket.public_access.set");
    assert_eq!(
        public_bucket_event.audit_event_id,
        public_bucket_response.audit_event_id
    );
    let public_bucket_details = details(public_bucket_event);
    assert_eq!(public_bucket_details["allow_public_read"], true);
    assert_eq!(public_bucket_details["expected_generation"], 1);

    let region_event = find_event("admin.region.create");
    assert_eq!(region_event.audit_event_id, region_response.audit_event_id);
    assert_eq!(region_event.resource_id, "region:eu-west-1");
    let region_details = details(region_event);
    assert_eq!(region_details["state"], "joining");
    assert_eq!(region_details["placement_weight"], 100);

    let active_region_event = find_event("admin.region.activate");
    assert_eq!(
        active_region_event.audit_event_id,
        active_region_response.audit_event_id
    );
    let active_region_details = details(active_region_event);
    assert_eq!(active_region_details["state"], "active");
    assert!(active_region_details["activation_checkpoint"].is_object());

    let host_alias_event = find_event("admin.host_alias.create");
    assert_eq!(
        host_alias_event.audit_event_id,
        host_alias_response.audit_event_id
    );
    assert_eq!(host_alias_event.resource_id, "host_alias:audit.example.com");
    let host_alias_details = details(host_alias_event);
    assert_eq!(host_alias_details["hostname"], "audit.example.com");
    assert_eq!(host_alias_details["prefix"], "public/");
}
