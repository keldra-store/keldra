use super::*;

async fn create_active_region_for_bucket_move(
    node: &AdminNode,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
    label: &str,
    region: &str,
) {
    let region_descriptor = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context(&format!("{label}-create-region"), 0)),
                region: region.to_string(),
                public_base_url: format!("https://{region}.anvil-storage.test"),
                virtual_host_suffix: format!("{region}.anvil-storage.test"),
                placement_weight: 100,
                default_cell: format!("{label}-cell"),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();

    prepare_active_region_dependencies(
        client,
        token,
        label,
        region,
        &format!("{label}-cell"),
        &format!("{label}-node"),
    )
    .await;

    client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context(
                    &format!("{label}-activate-region"),
                    region_descriptor.generation,
                )),
                region: region.to_string(),
                activation_checkpoint_json: activation_checkpoint_json_from_existing_streams(
                    node, region,
                )
                .await,
            }),
            token,
        ))
        .await
        .unwrap();
}

fn delete_routing_projection_row(node: &AdminNode, stream_family: &str, record_key: &str) {
    let row_key = anvil::core_store::core_meta_tuple_key(&[
        anvil::core_store::CoreMetaTuplePart::Utf8("mesh-directory-projection"),
        anvil::core_store::CoreMetaTuplePart::Utf8(stream_family),
        anvil::core_store::CoreMetaTuplePart::Utf8(record_key),
    ])
    .unwrap();
    anvil::core_store::CoreMetaStore::open(node.state.storage.core_store_meta_path())
        .unwrap()
        .delete(
            anvil::core_store::CF_MESH,
            anvil::core_store::TABLE_MESH_PARTITION_ROW,
            &row_key,
        )
        .unwrap();
}

#[tokio::test]
async fn mesh_bucket_move_requires_routing_and_bucket_zanzibar_permissions() {
    let node = spawn_admin_node().await;
    let admin_token = admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let mut mesh_client =
        anvil::anvil_api::mesh_control_service_client::MeshControlServiceClient::connect(
            node.admin_url.clone(),
        )
        .await
        .unwrap();

    let tenant = admin_client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("mesh-move-tenant", 0)),
                name: "mesh-move-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner()
        .tenant
        .unwrap();
    let bucket = admin_client
        .create_bucket_admin(with_auth(
            tonic::Request::new(CreateBucketAdminRequest {
                context: Some(context("mesh-move-bucket", 0)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: "movable-assets".to_string(),
                region: "eu-west-1".to_string(),
            }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket
        .unwrap();
    assert_eq!(bucket.region, "eu-west-1");

    create_active_region_for_bucket_move(
        &node,
        &mut admin_client,
        &admin_token,
        "mesh-move-target",
        "us-east-1",
    )
    .await;

    let router_only = "router-only-principal";
    node.state
        .persistence
        .write_authz_tuple(
            anvil::system_realm::SYSTEM_STORAGE_TENANT_ID,
            &anvil::system_realm::system_namespace(),
            anvil::system_realm::SYSTEM_OBJECT_ID,
            "manage_partitions_grant",
            anvil::access_control::APP_SUBJECT_KIND,
            router_only,
            "",
            "add",
            "test",
            "grant routing only without bucket manage",
        )
        .await
        .unwrap();
    let router_only_token = node
        .state
        .jwt_manager
        .mint_token(
            router_only.to_string(),
            anvil::system_realm::SYSTEM_STORAGE_TENANT_ID,
        )
        .unwrap();
    let denied = mesh_client
        .move_bucket(with_auth(
            tonic::Request::new(MoveBucketRequest {
                bucket_name: "movable-assets".to_string(),
                target_region_id: "us-east-1".to_string(),
                options: None,
                tenant_id: tenant.tenant_id.clone(),
            }),
            &router_only_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    mesh_client
        .move_bucket(with_auth(
            tonic::Request::new(MoveBucketRequest {
                bucket_name: "movable-assets".to_string(),
                target_region_id: "us-east-1".to_string(),
                options: None,
                tenant_id: tenant.tenant_id.clone(),
            }),
            &admin_token,
        ))
        .await
        .unwrap();

    let tenant_id = tenant.tenant_id.parse::<i64>().unwrap();
    let moved = node
        .state
        .persistence
        .get_bucket_by_name(tenant_id, "movable-assets")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(moved.region, "us-east-1");
    let locator_key = anvil::mesh_directory::BucketLocatorKey::new(
        anvil::mesh_directory::TenantId::new(tenant.tenant_id).unwrap(),
        anvil::mesh_directory::BucketName::canonicalize("movable-assets").unwrap(),
    );
    let locator = anvil::mesh_directory::read_bucket_locator(&node.state.storage, &locator_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(locator.home_region.as_str(), "us-east-1");
}

#[tokio::test]
async fn admin_tenant_app_and_bucket_workflow_issues_usable_credentials() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant = admin_client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("admin-create-tenant", 0)),
                name: "operator-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .tenant
        .unwrap();
    assert_eq!(tenant.name, "operator-tenant");

    let app_secret = admin_client
        .create_application(with_auth(
            tonic::Request::new(CreateApplicationRequest {
                context: Some(context("admin-create-app", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "publisher".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(app_secret.tenant_id, tenant.tenant_id);
    assert_eq!(app_secret.app_name, "publisher");
    assert!(app_secret.client_id.starts_with("app_"));
    assert!(app_secret.client_secret.starts_with("secret_"));

    let mut auth_client = AuthServiceClient::connect(node.public_url.clone())
        .await
        .unwrap();
    let token_response = auth_client
        .get_access_token(tonic::Request::new(GetAccessTokenRequest {
            client_id: app_secret.client_id.clone(),
            client_secret: app_secret.client_secret.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!token_response.access_token.is_empty());

    let bucket = admin_client
        .create_bucket_admin(with_auth(
            tonic::Request::new(CreateBucketAdminRequest {
                context: Some(context("admin-create-bucket", 0)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: "release-assets".to_string(),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket
        .unwrap();
    assert_eq!(bucket.name, "release-assets");
    assert!(!bucket.is_public_read);

    let public_bucket = admin_client
        .set_bucket_public_access_admin(with_auth(
            tonic::Request::new(SetBucketPublicAccessAdminRequest {
                context: Some(context("admin-public-bucket", 1)),
                tenant_id: tenant.tenant_id,
                bucket_name: "release-assets".to_string(),
                allow_public_read: true,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket
        .unwrap();
    assert!(public_bucket.is_public_read);
}

#[tokio::test]
async fn admin_routing_records_list_and_repair_mesh_locators() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant = node
        .state
        .persistence
        .create_tenant("route-tenant", "unused")
        .await
        .unwrap();
    node.state
        .persistence
        .create_bucket(tenant.id, "route-bucket", "eu-west-1")
        .await
        .unwrap();

    let records = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 0,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert!(records.iter().any(|record| record.family == 1
        && record.record_key == "route-tenant"
        && record.payload_json.contains("\"tenant_name\"")));
    let bucket_record = records
        .iter()
        .find(|record| {
            record.family == 3 && record.record_key == format!("{}/route-bucket", tenant.id)
        })
        .cloned()
        .expect("bucket locator record should be listed");

    delete_routing_projection_row(
        &node,
        "bucket_locator",
        &format!("{}/route-bucket", tenant.id),
    );

    let missing_after_delete = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 3,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert!(
        missing_after_delete
            .iter()
            .all(|record| record.record_key != format!("{}/route-bucket", tenant.id)),
        "routing listings must not hide a missing projection with a control-stream scan"
    );
    let diagnostics_after_delete = client
        .list_diagnostics(with_auth(
            tonic::Request::new(ListDiagnosticsRequest {
                request_id: "req-route-diagnostics-after-delete".to_string(),
                source: "mesh_routing_projection".to_string(),
                tenant_id: String::new(),
                bucket_name: String::new(),
                index_name: String::new(),
                severity: String::new(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    assert!(diagnostics_after_delete.iter().any(|diagnostic| {
        diagnostic.code == "mesh_control_projection_missing_record"
            && diagnostic
                .details_json
                .contains(&format!("{}/route-bucket", tenant.id))
    }));
    let route_diagnostic = diagnostics_after_delete
        .iter()
        .find(|diagnostic| {
            diagnostic.code == "mesh_control_projection_missing_record"
                && diagnostic
                    .details_json
                    .contains(&format!("{}/route-bucket", tenant.id))
        })
        .expect("routing projection diagnostic should name the missing record");
    let route_diagnostic_details: serde_json::Value =
        serde_json::from_str(&route_diagnostic.details_json).unwrap();
    assert_eq!(
        route_diagnostic_details["descriptor_key"],
        bucket_record.descriptor_key
    );
    assert_eq!(route_diagnostic_details["repair_safe"], true);
    assert_eq!(
        route_diagnostic_details["proposed_action"],
        "repair_routing_record_from_control_stream"
    );
    assert!(
        route_diagnostic_details["stream_sequence"]
            .as_u64()
            .is_some()
    );
    assert!(
        route_diagnostic_details["stream_digest"]
            .as_str()
            .unwrap()
            .starts_with("blake3:")
    );

    let repaired = client
        .repair_routing_record(with_auth(
            tonic::Request::new(RepairRoutingRecordRequest {
                context: Some(context("repair-bucket-routing-record", 1)),
                family: 3,
                record_key: format!("{}/route-bucket", tenant.id),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repaired.generation, 1);
    assert_eq!(repaired.resource_id, bucket_record.descriptor_key);

    let listed_after_repair = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 3,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert!(
        listed_after_repair
            .iter()
            .any(|record| record.record_key == format!("{}/route-bucket", tenant.id))
    );
    delete_routing_projection_row(
        &node,
        "bucket_locator",
        &format!("{}/route-bucket", tenant.id),
    );
    let projection_repair = client
        .run_repair(with_auth(
            tonic::Request::new(RunRepairRequest {
                context: Some(context("repair-routing-projection", 0)),
                repair_kind: 5,
                tenant_id: String::new(),
                bucket_name: String::new(),
                index_name: String::new(),
                derived_index_id: String::new(),
                database_id: String::new(),
                rebuild: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection_repair.status, "completed");
    assert!(
        projection_repair
            .details_json
            .contains("\"repair_kind\":\"mesh_routing_projection\"")
    );
    assert!(
        projection_repair
            .details_json
            .contains("\"repaired_count\":1")
    );
    assert_eq!(projection_repair.findings.len(), 1);
    assert_eq!(
        projection_repair.findings[0].code,
        "mesh_control_projection_missing_record"
    );
    assert_eq!(
        projection_repair.findings[0].proposed_action,
        "RebuildDerivedIndex"
    );
    assert!(
        projection_repair.findings[0]
            .evidence_json
            .contains(&bucket_record.descriptor_key)
    );
    let projection_repair_details: serde_json::Value =
        serde_json::from_str(&projection_repair.details_json).unwrap();
    assert_eq!(projection_repair_details["repaired_count"], 1);
    assert_eq!(
        projection_repair_details["repaired_records"][0]["descriptor_key"],
        bucket_record.descriptor_key
    );
    assert_eq!(
        projection_repair_details["repaired_records"][0]["repair_result"]["applied_action"],
        "repair_routing_record_from_control_stream"
    );
    let diagnostics_after_repair = client
        .list_diagnostics(with_auth(
            tonic::Request::new(ListDiagnosticsRequest {
                request_id: "req-route-diagnostics-after-repair".to_string(),
                source: "mesh_routing_projection".to_string(),
                tenant_id: String::new(),
                bucket_name: String::new(),
                index_name: String::new(),
                severity: String::new(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    assert!(
        !diagnostics_after_repair.iter().any(|diagnostic| {
            diagnostic
                .details_json
                .contains(&format!("{}/route-bucket", tenant.id))
        }),
        "routing projection diagnostics should clear after stream-backed repair"
    );
}
