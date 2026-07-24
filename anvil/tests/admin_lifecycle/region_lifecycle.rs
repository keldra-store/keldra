use super::*;

#[tokio::test]
async fn admin_lifecycle_rejects_invalid_region_cell_and_node_transitions() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let region = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("create-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(region.state, 1);

    let drain_joining_region = client
        .drain_region(with_auth(
            tonic::Request::new(DrainRegionRequest {
                context: Some(context("drain-joining-region", region.generation)),
                region: "eu-west-1".to_string(),
                default_disposition: 1,
                bucket_overrides: vec![],
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(drain_joining_region.code(), Code::FailedPrecondition);

    let cell = client
        .register_cell(with_auth(
            tonic::Request::new(RegisterCellRequest {
                context: Some(context("register-cell", 0)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
                failure_domain: "rack-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();

    let drain_joining_cell = client
        .drain_cell(with_auth(
            tonic::Request::new(DrainCellRequest {
                context: Some(context("drain-joining-cell", cell.generation)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(drain_joining_cell.code(), Code::FailedPrecondition);

    let cell = client
        .activate_cell(with_auth(
            tonic::Request::new(ActivateCellRequest {
                context: Some(context("activate-cell", cell.generation)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    assert_eq!(cell.state, 2);

    let registered_node = client
        .register_node(with_auth(
            tonic::Request::new(RegisterNodeRequest {
                context: Some(context("register-node", 0)),
                node_id: "node-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                receipt_signing_public_key: test_receipt_signing_public_key(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![1, 6],
                capacity_json: "{}".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(registered_node.state, 1);

    let drain_joining_node = client
        .drain_node(with_auth(
            tonic::Request::new(DrainNodeRequest {
                context: Some(context("drain-joining-node", registered_node.generation)),
                node_id: "node-a".to_string(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(drain_joining_node.code(), Code::FailedPrecondition);

    let active_node = client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context("activate-node", registered_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(active_node.state, 2);

    let not_reached = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context(
                    "activate-region-missing-checkpoint",
                    region.generation,
                )),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: missing_activation_checkpoint_json(
                    "mesh-test",
                    "eu-west-1",
                ),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(not_reached.code(), Code::FailedPrecondition);
    assert!(
        not_reached
            .message()
            .contains("ActivationCheckpointNotReached")
    );

    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("activate-region", region.generation)),
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
    assert_eq!(region.state, 2);

    let read_only_region = client
        .set_region_read_only(with_auth(
            tonic::Request::new(SetRegionReadOnlyRequest {
                context: Some(context("set-region-read-only", region.generation)),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(read_only_region.state, 3);
    assert_eq!(read_only_region.generation, region.generation + 1);

    let listed_regions = client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed_regions.regions.len(), 1);
    assert_eq!(listed_regions.regions[0].state, 3);
    assert_eq!(
        listed_regions.regions[0].generation,
        read_only_region.generation
    );

    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context(
                    "reactivate-read-only-region",
                    read_only_region.generation,
                )),
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
    assert_eq!(region.state, 2);

    let remove_active_region = client
        .remove_region(with_auth(
            tonic::Request::new(RemoveRegionRequest {
                context: Some(context("remove-active-region", region.generation)),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(remove_active_region.code(), Code::FailedPrecondition);

    let remove_active_node = client
        .remove_node(with_auth(
            tonic::Request::new(RemoveNodeRequest {
                context: Some(context("remove-active-node", active_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(remove_active_node.code(), Code::FailedPrecondition);

    let offline_node = client
        .force_offline_node(with_auth(
            tonic::Request::new(ForceOfflineNodeRequest {
                context: Some(context("force-offline-node", active_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(offline_node.state, 7);
    assert_eq!(offline_node.generation, active_node.generation + 1);

    let listed_offline = client
        .list_nodes(with_auth(
            tonic::Request::new(ListNodesRequest {
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed_offline.nodes.len(), 1);
    assert_eq!(listed_offline.nodes[0].state, 7);
    assert_eq!(listed_offline.nodes[0].generation, offline_node.generation);

    let active_node = client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context("reactivate-offline-node", offline_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(active_node.state, 2);

    let drained = client
        .drain_node(with_auth(
            tonic::Request::new(DrainNodeRequest {
                context: Some(context("drain-node", active_node.generation)),
                node_id: "node-a".to_string(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(drained.state, 4);

    let listed = client
        .list_nodes(with_auth(
            tonic::Request::new(ListNodesRequest {
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.nodes.len(), 1);
    assert_eq!(listed.nodes[0].state, 4);

    let lifecycle_diagnostics = client
        .list_diagnostics(with_auth(
            tonic::Request::new(ListDiagnosticsRequest {
                request_id: "req-list-lifecycle-diagnostics".to_string(),
                source: "mesh_lifecycle".to_string(),
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
    let node_diagnostic = lifecycle_diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "mesh_node_not_active")
        .expect("draining node should be reported in lifecycle diagnostics");
    let node_diagnostic_details: serde_json::Value =
        serde_json::from_str(&node_diagnostic.details_json).unwrap();
    assert_eq!(
        node_diagnostic_details["runtime_ownership_blocker_count"],
        0
    );
    assert!(node_diagnostic_details["runtime_ownership_blockers"].is_array());
    assert_eq!(
        node_diagnostic_details["ownership_repair"]["proposed_action"],
        "no_runtime_ownership_repair_needed"
    );
}

#[tokio::test]
async fn admin_region_drain_applies_bucket_dispositions_and_exceptions() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let region = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("create-drain-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    let cell = client
        .register_cell(with_auth(
            tonic::Request::new(RegisterCellRequest {
                context: Some(context("register-drain-cell", 0)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
                failure_domain: "rack-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    client
        .activate_cell(with_auth(
            tonic::Request::new(ActivateCellRequest {
                context: Some(context("activate-drain-cell", cell.generation)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap();
    let local_node_id = node.state.config.node_id.clone();
    let node_descriptor = client
        .register_node(with_auth(
            tonic::Request::new(RegisterNodeRequest {
                context: Some(context("register-drain-node", 0)),
                node_id: local_node_id.clone(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                receipt_signing_public_key: node
                    .state
                    .core_store
                    .local_receipt_signing_public_key(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![1, 6],
                capacity_json: "{}".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context("activate-drain-node", node_descriptor.generation)),
                node_id: local_node_id,
            }),
            &token,
        ))
        .await
        .unwrap();
    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("activate-drain-region", region.generation)),
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

    let tenant = node
        .state
        .persistence
        .create_tenant("tenant-a", "tenant-a-idempotency")
        .await
        .unwrap();
    node.state
        .persistence
        .create_bucket(tenant.id, "docs", "eu-west-1")
        .await
        .unwrap();

    let drained = client
        .drain_region(with_auth(
            tonic::Request::new(DrainRegionRequest {
                context: Some(context("drain-region-with-exception", region.generation)),
                region: "eu-west-1".to_string(),
                default_disposition: 1,
                bucket_overrides: vec![BucketDrainOverride {
                    tenant_id: tenant.id.to_string(),
                    bucket_name: "docs".to_string(),
                    disposition: 2,
                    reason: "customer-approved delayed migration".to_string(),
                    expires_at: "2026-08-02T00:00:00Z".to_string(),
                }],
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(drained.state, 4);

    let locator = node
        .state
        .persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(format!("{:?}", locator.status), "ReadOnly");
    let exceptions =
        anvil::mesh_lifecycle::list_bucket_drain_exceptions(&node.state.storage, Some("eu-west-1"))
            .await
            .unwrap();
    assert_eq!(exceptions.len(), 1);
    assert_eq!(exceptions[0].bucket_name, "docs");

    let audit = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "list-drain-audit".to_string(),
                principal_id: String::new(),
                resource_id: "region:eu-west-1".to_string(),
                action: "admin.region.drain".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(audit.events.len(), 1);
    assert!(
        audit.events[0]
            .details_json
            .contains("bucket_disposition_decisions")
    );
    let disposition_audit = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "list-bucket-disposition-audit".to_string(),
                principal_id: String::new(),
                resource_id: format!("tenant:{}:bucket:docs:region:eu-west-1", tenant.id),
                action: "admin.region.bucket_disposition".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(disposition_audit.events.len(), 1);
    assert!(
        disposition_audit.events[0]
            .details_json
            .contains("remain_proxy_only")
    );
}
