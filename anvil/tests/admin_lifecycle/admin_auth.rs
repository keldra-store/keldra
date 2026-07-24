use super::*;

#[tokio::test]
async fn admin_authorisation_uses_zanzibar_system_realm() {
    let node = spawn_admin_node().await;
    let admin_token = admin_token(&node);
    let claims = node.state.jwt_manager.verify_token(&admin_token).unwrap();
    let allowed = anvil::system_realm::check_admin_relation(
        &node.state.storage,
        &node.state.config.mesh_id,
        &claims,
        anvil::system_realm::SystemAdminRelation::ManageRegions,
    )
    .await
    .unwrap();
    assert!(
        allowed,
        "bootstrap-created system realm tuple must authorize admin"
    );

    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let response = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.regions.is_empty());
}

#[test]
fn admin_rpc_relation_mapping_is_complete() {
    let mapping = anvil::services::admin::admin_rpc_relation_mapping();
    let names: std::collections::HashSet<_> = mapping.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        names.len(),
        mapping.len(),
        "admin RPC mapping must not contain duplicates"
    );

    let expected = [
        "CreateTenant",
        "CreateApplication",
        "RotateApplicationSecret",
        "GrantApplicationPolicy",
        "RevokeApplicationPolicy",
        "RotateSecretEncryptionKey",
        "CreateBucketAdmin",
        "SetBucketPublicAccessAdmin",
        "CreateHostAlias",
        "ActivateHostAlias",
        "SuspendHostAlias",
        "DeleteHostAlias",
        "ReadHostAlias",
        "ListHostAliases",
        "CreateRegion",
        "ActivateRegion",
        "SetRegionReadOnly",
        "DrainRegion",
        "RemoveRegion",
        "ListRegions",
        "RegisterCell",
        "ActivateCell",
        "DrainCell",
        "RemoveCell",
        "ListCells",
        "RegisterNode",
        "ActivateNode",
        "DrainNode",
        "ForceOfflineNode",
        "RemoveNode",
        "ListNodes",
        "ListRoutingRecords",
        "RepairRoutingRecord",
        "RunRepair",
        "ListDiagnostics",
        "ListAuditEvents",
    ];
    for name in expected {
        assert!(
            names.contains(name),
            "missing admin RPC relation mapping for {name}"
        );
    }
}

#[tokio::test]
async fn admin_without_required_relation_is_denied() {
    let node = spawn_admin_node().await;
    let non_admin_token = non_admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let denied = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &non_admin_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn admin_with_required_relation_is_allowed() {
    let node = spawn_admin_node().await;
    let admin_token = admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &admin_token,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn tenant_data_plane_credential_does_not_grant_admin() {
    let node = spawn_admin_node().await;
    let token = non_admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let denied = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn admin_interceptor_rejects_missing_auth() {
    let node = spawn_admin_node().await;
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let err = admin_client
        .list_regions(tonic::Request::new(ListRegionsRequest { page: None }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn admin_interceptor_rejects_public_data_plane_only_credential() {
    let node = spawn_admin_node().await;
    let token = non_admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let err = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[test]
fn public_admin_bind_requires_explicit_unsafe_opt_in() {
    for addr in ["0.0.0.0:50052", "[::]:50052", "192.168.1.10:50052"] {
        let config = anvil::config::Config {
            admin_listen_addr: addr.to_string(),
            allow_public_admin_listener: false,
            ..Default::default()
        };
        assert!(config.validate_admin_listener_bind().is_err(), "{addr}");
    }

    for addr in ["127.0.0.1:50052", "[::1]:50052"] {
        let config = anvil::config::Config {
            admin_listen_addr: addr.to_string(),
            allow_public_admin_listener: false,
            ..Default::default()
        };
        config.validate_admin_listener_bind().unwrap();
    }

    let config = anvil::config::Config {
        admin_listen_addr: "0.0.0.0:50052".to_string(),
        allow_public_admin_listener: true,
        ..Default::default()
    };
    config.validate_admin_listener_bind().unwrap();
}

#[tokio::test]
async fn admin_service_is_absent_public_present_admin_and_requires_auth() {
    let node = spawn_admin_node().await;
    let admin_token = admin_token(&node);
    let non_admin_token = non_admin_token(&node);

    let mut public_client = AdminServiceClient::connect(node.public_url.clone())
        .await
        .unwrap();
    let public_err = public_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &admin_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(public_err.code(), Code::Unimplemented);

    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let unauthenticated = admin_client
        .list_regions(tonic::Request::new(ListRegionsRequest { page: None }))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let denied = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &non_admin_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let response = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.regions.is_empty());
}

#[tokio::test]
async fn admin_policy_and_secret_key_rotation_use_admin_api() {
    let node = spawn_admin_node().await;
    let admin_token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant = client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("policy-tenant", 0)),
                name: "policy-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner()
        .tenant
        .unwrap();

    let app = client
        .create_application(with_auth(
            tonic::Request::new(CreateApplicationRequest {
                context: Some(context("policy-app", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "policy-app".to_string(),
            }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!app.client_id.is_empty());
    assert!(!app.client_secret.is_empty());
    let tenant_resource = format!("tenant:{}", tenant.tenant_id);

    client
        .grant_application_policy(with_auth(
            tonic::Request::new(GrantApplicationPolicyRequest {
                context: Some(context("policy-grant", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "policy-app".to_string(),
                action: "bucket:create".to_string(),
                resource: tenant_resource.clone(),
            }),
            &admin_token,
        ))
        .await
        .unwrap();
    let wildcard_err = client
        .grant_application_policy(with_auth(
            tonic::Request::new(GrantApplicationPolicyRequest {
                context: Some(context("policy-wildcard-denied", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "policy-app".to_string(),
                action: "*".to_string(),
                resource: "*".to_string(),
            }),
            &admin_token,
        ))
        .await
        .expect_err("admin policy grants must reject wildcard authority");
    assert_eq!(wildcard_err.code(), tonic::Code::PermissionDenied);
    let revision = node
        .state
        .persistence
        .current_control_collection_revision()
        .await
        .unwrap();
    let app_record = node
        .state
        .persistence
        .page_apps_for_tenant(tenant.tenant_id.parse().unwrap(), &revision, None, 100)
        .await
        .unwrap()
        .apps
        .into_iter()
        .find(|app| app.name == "policy-app")
        .unwrap();
    let app_claims = anvil::auth::Claims {
        sub: app_record.id.to_string(),
        exp: usize::MAX,
        tenant_id: tenant.tenant_id.parse().unwrap(),
        jti: None,
    };
    assert!(
        anvil::access_control::action_allows(
            &node.state.storage,
            &node.state.persistence,
            &app_claims,
            anvil::permissions::AnvilAction::BucketCreate,
            &tenant_resource,
        )
        .await
        .unwrap()
    );

    client
        .revoke_application_policy(with_auth(
            tonic::Request::new(RevokeApplicationPolicyRequest {
                context: Some(context("policy-revoke", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "policy-app".to_string(),
                action: "bucket:create".to_string(),
                resource: tenant_resource.clone(),
            }),
            &admin_token,
        ))
        .await
        .unwrap();
    assert!(
        !anvil::access_control::action_allows(
            &node.state.storage,
            &node.state.persistence,
            &app_claims,
            anvil::permissions::AnvilAction::BucketCreate,
            &tenant_resource,
        )
        .await
        .unwrap()
    );

    let rotation = client
        .rotate_secret_encryption_key(with_auth(
            tonic::Request::new(RotateSecretEncryptionKeyRequest {
                context: Some(context("secret-rotation-dry-run", 0)),
                dry_run: true,
            }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(rotation.dry_run);
    assert_eq!(rotation.active_key_id, "primary");
    assert!(rotation.app_secrets_examined >= 1);
}
