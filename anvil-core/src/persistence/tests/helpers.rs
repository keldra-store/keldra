use super::*;

pub(super) async fn register_active_mesh_placement(
    persistence: &Persistence,
) -> (
    crate::mesh_lifecycle::RegionDescriptor,
    crate::mesh_lifecycle::CellDescriptor,
    crate::mesh_lifecycle::NodeDescriptor,
) {
    let region = persistence
        .create_region_descriptor(crate::mesh_lifecycle::CreateRegionDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            public_base_url: "https://test-region.anvil-storage.test".to_string(),
            virtual_host_suffix: "test-region.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("default".to_string()),
        })
        .await
        .unwrap();
    let cell = persistence
        .register_cell_descriptor(crate::mesh_lifecycle::RegisterCellDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        })
        .await
        .unwrap();
    let cell = persistence
        .transition_cell_descriptor(
            "test-region",
            "default",
            cell.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    let region = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    let node = persistence
        .register_node_descriptor(crate::mesh_lifecycle::RegisterNodeDescriptor {
            mesh_id: "default".to_string(),
            node_id: "test-node".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            receipt_signing_public_key: crate::node_signing::NodeSigningKeypair::generate()
                .unwrap()
                .public_key_bytes()
                .to_vec(),
            public_api_addr: "test-node".to_string(),
            capabilities: vec![
                crate::mesh_lifecycle::NodeCapability::Object,
                crate::mesh_lifecycle::NodeCapability::Admin,
            ],
            capacity_json: "{}".to_string(),
        })
        .await
        .unwrap();
    let node = persistence
        .transition_node_descriptor(
            "test-node",
            node.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
            None,
        )
        .await
        .unwrap();
    (region, cell, node)
}

pub(super) fn payload_ref(label: &str, logical_size: u64) -> crate::core_store::CoreObjectRef {
    crate::core_store::CoreObjectRef::test_unlocated(
        format!(
            "sha256:{}",
            hex::encode(blake3::hash(label.as_bytes()).as_bytes())
        ),
        logical_size,
        format!("manifest:{label}"),
    )
}
