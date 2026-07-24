use super::record_proto;
use super::*;
use std::collections::BTreeMap;
use tempfile::tempdir;

fn test_receipt_signing_public_key() -> Vec<u8> {
    crate::node_signing::NodeSigningKeypair::generate()
        .unwrap()
        .public_key_bytes()
        .to_vec()
}

#[test]
fn node_state_machine_rejects_invalid_transitions() {
    validate_node_transition(LifecycleState::Joining, LifecycleState::Active).unwrap();
    assert!(validate_node_transition(LifecycleState::Joining, LifecycleState::Draining).is_err());
    assert!(validate_node_transition(LifecycleState::Active, LifecycleState::Removed).is_err());
    assert!(validate_node_transition(LifecycleState::Removed, LifecycleState::Active).is_err());
}

#[test]
fn region_state_machine_rejects_invalid_transitions() {
    validate_region_transition(LifecycleState::Joining, LifecycleState::Active).unwrap();
    validate_region_transition(LifecycleState::Active, LifecycleState::ReadOnly).unwrap();
    assert!(validate_region_transition(LifecycleState::Joining, LifecycleState::Draining).is_err());
    assert!(validate_region_transition(LifecycleState::Active, LifecycleState::Removed).is_err());
}

#[tokio::test]
async fn activate_region_rejects_missing_activation_checkpoint_stream() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    let checkpoint = checkpoint_with_stream("bucket_locator", "0a7f", 1, digest_for(b"record"));

    let err = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LifecycleError::ActivationCheckpointNotReached { .. }
    ));
    assert_eq!(
        read_state(&storage)
            .await
            .unwrap()
            .regions
            .get("eu-west-1")
            .unwrap()
            .state,
        LifecycleState::Joining
    );
}

#[tokio::test]
async fn activate_region_rejects_mismatched_activation_checkpoint_digest() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    append_control_record(&storage, "bucket_locator", "0a7f", 1, digest_for(b"actual")).await;
    write_control_checkpoint_record(
        &storage,
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        1,
        digest_for(b"actual"),
    )
    .await;
    let checkpoint = checkpoint_with_stream("bucket_locator", "0a7f", 1, digest_for(b"expected"));

    let err = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LifecycleError::ActivationCheckpointNotReached { .. }
    ));
}

#[tokio::test]
async fn activate_region_rejects_checkpoint_that_omits_existing_control_stream() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    append_control_record(&storage, "bucket_locator", "0a7f", 1, digest_for(b"record")).await;
    let checkpoint = ActivationCheckpoint {
        schema: ACTIVATION_CHECKPOINT_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        required_streams: vec![],
    };

    let err = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LifecycleError::ActivationCheckpointNotReached {
            stream_family,
            partition,
            ..
        } if stream_family == "bucket_locator" && partition == "0a7f"
    ));
}

#[tokio::test]
async fn activate_region_accepts_reached_activation_checkpoint() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    let cell = register_test_cell(&storage).await;
    let _cell = transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let node = register_test_node(&storage).await;
    let _node = transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    let digest = digest_for(b"record");
    append_control_record(&storage, "bucket_locator", "0a7f", 1, digest.clone()).await;
    write_control_checkpoint_record(
        &storage,
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        1,
        digest.clone(),
    )
    .await;
    let checkpoint = checkpoint_with_stream("bucket_locator", "0a7f", 1, digest);

    let active = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap();
    assert_eq!(active.state, LifecycleState::Active);
}

#[tokio::test]
async fn region_activation_requires_active_cell_and_node() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    let checkpoint = ActivationCheckpoint {
        schema: ACTIVATION_CHECKPOINT_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        required_streams: vec![],
    };

    let missing_cell = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(
        matches!(missing_cell, LifecycleError::InvalidArgument(message) if message.contains("active cell"))
    );

    let cell = register_test_cell(&storage).await;
    transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let missing_node = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(
        matches!(missing_node, LifecycleError::InvalidArgument(message) if message.contains("active node"))
    );

    let node = register_test_node(&storage).await;
    transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    let active = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap();
    assert_eq!(active.state, LifecycleState::Active);
}

#[tokio::test]
async fn writable_placement_rejects_non_active_region() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    assert!(
        ensure_region_accepts_new_writes(&storage, "legacy-region")
            .await
            .is_ok()
    );
    create_test_region(&storage).await;

    assert!(
        ensure_region_accepts_new_writes(&storage, "eu-west-1")
            .await
            .is_err()
    );
    assert!(
        ensure_region_accepts_new_writes(&storage, "legacy-region")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn writable_placement_rejects_stale_or_inactive_region_cell_and_node() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let (region, _cell, _node) = create_active_placement_model(&storage).await;

    ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
        .await
        .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "us-east-1", "cell-a", "node-a")
            .await
            .is_err()
    );
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-b", "node-a")
            .await
            .is_err()
    );
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-b")
            .await
            .is_err()
    );

    transition_region(
        &storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Draining,
    )
    .await
    .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
            .await
            .is_err()
    );

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let (_region, cell, _node) = create_active_placement_model(&storage).await;
    transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Draining,
    )
    .await
    .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
            .await
            .is_err()
    );

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let (_region, _cell, node) = create_active_placement_model(&storage).await;
    transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Draining,
        Some(NodeDrainDescriptor {
            started_at: timestamp_now(),
            graceful_timeout_ms: 1000,
            force_after_timeout: false,
        }),
    )
    .await
    .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lifecycle_store_persists_descriptors_and_enforces_transitions() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    let region = create_region(
        &storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("cell-a".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(region.state, LifecycleState::Joining);

    let cell = register_cell(
        &storage,
        RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        },
    )
    .await
    .unwrap();
    let cell = transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    assert_eq!(cell.state, LifecycleState::Active);

    let region = transition_region(
        &storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    assert_eq!(region.state, LifecycleState::Active);

    let node = register_node(
        &storage,
        RegisterNodeDescriptor {
            mesh_id: "mesh-a".to_string(),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            receipt_signing_public_key: test_receipt_signing_public_key(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
            capacity_json: "{}".to_string(),
        },
    )
    .await
    .unwrap();
    assert!(
        transition_node(
            &storage,
            "node-a",
            node.generation,
            LifecycleState::Draining,
            None,
        )
        .await
        .is_err()
    );

    let node = transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    assert!(
        transition_node(
            &storage,
            "node-a",
            node.generation,
            LifecycleState::Removed,
            None,
        )
        .await
        .is_err()
    );
    let node = transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Draining,
        Some(NodeDrainDescriptor {
            started_at: timestamp_now(),
            graceful_timeout_ms: 1000,
            force_after_timeout: false,
        }),
    )
    .await
    .unwrap();
    assert_eq!(node.state, LifecycleState::Draining);

    let replayed = read_state(&storage).await.unwrap();
    assert_eq!(replayed.nodes["node-a"].state, LifecycleState::Draining);
}

#[tokio::test]
async fn lifecycle_read_model_replays_control_streams_as_source_of_truth() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    let stale_region = RegionDescriptor {
        schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        state: LifecycleState::Joining,
        public_base_url: "https://stale.example.test".to_string(),
        virtual_host_suffix: "stale.example.test".to_string(),
        placement_weight: 1,
        default_cell: None,
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:00:00Z".to_string(),
        generation: 1,
    };
    write_state(
        &storage,
        &MeshLifecycleState {
            regions: BTreeMap::from([("eu-west-1".to_string(), stale_region)]),
            ..MeshLifecycleState::default()
        },
    )
    .await
    .unwrap();

    let region = RegionDescriptor {
        schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        state: LifecycleState::Active,
        public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
        virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
        placement_weight: 100,
        default_cell: Some("cell-a".to_string()),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:01:00Z".to_string(),
        generation: 2,
    };
    append_lifecycle_descriptor(
        &storage,
        REGION_DESCRIPTOR_STREAM_FAMILY,
        "eu-west-1",
        1,
        &region,
    )
    .await;

    let cell = CellDescriptor {
        schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        state: LifecycleState::Active,
        placement_weight: 100,
        failure_domain: "rack-a".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:01:00Z".to_string(),
        generation: 2,
    };
    append_lifecycle_descriptor(
        &storage,
        CELL_DESCRIPTOR_STREAM_FAMILY,
        "eu-west-1/cell-a",
        1,
        &cell,
    )
    .await;

    let node = NodeDescriptor {
        schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        node_id: "node-a".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        receipt_signing_public_key: test_receipt_signing_public_key(),
        public_api_addr: "http://127.0.0.1:50051".to_string(),
        capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
        capacity_json_hash: blake3::hash(b"{}").to_hex().to_string(),
        state: LifecycleState::Active,
        drain: None,
        last_heartbeat_at: Some("2026-07-02T00:01:00Z".to_string()),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:01:00Z".to_string(),
        generation: 2,
    };
    append_lifecycle_descriptor(
        &storage,
        NODE_DESCRIPTOR_STREAM_FAMILY,
        "eu-west-1/cell-a/node-a",
        1,
        &node,
    )
    .await;

    let replayed = read_state(&storage).await.unwrap();
    assert_eq!(
        replayed.regions["eu-west-1"].public_base_url,
        "https://eu-west-1.anvil-storage.test"
    );
    assert_eq!(replayed.regions["eu-west-1"].state, LifecycleState::Active);
    assert_eq!(
        replayed.cells["eu-west-1/cell-a"].state,
        LifecycleState::Active
    );
    assert_eq!(replayed.nodes["node-a"].state, LifecycleState::Active);

    delete_lifecycle_state_projection(&storage).await.unwrap();
    let replayed_without_projection = read_state(&storage).await.unwrap();
    assert_eq!(
        replayed_without_projection.regions["eu-west-1"].generation,
        2
    );
    assert_eq!(
        replayed_without_projection.cells["eu-west-1/cell-a"].generation,
        2
    );
    assert_eq!(replayed_without_projection.nodes["node-a"].generation, 2);
}

#[tokio::test]
async fn bootstrap_projection_is_visible_before_root_publication_begins() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();

    install_bootstrap_lifecycle_projection(
        &storage,
        &store,
        BootstrapMeshLifecycleProjection {
            regions: vec![CreateRegionDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: Some("cell-a".to_string()),
            }],
            cells: vec![RegisterCellDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
                failure_domain: "rack-a".to_string(),
            }],
            nodes: vec![RegisterNodeDescriptor {
                mesh_id: "mesh-a".to_string(),
                node_id: "node-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                receipt_signing_public_key: test_receipt_signing_public_key(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![NodeCapability::Metadata, NodeCapability::Object],
                capacity_json: "{}".to_string(),
            }],
        },
    )
    .unwrap();

    let state = read_state_with_core_store(&storage, &store).await.unwrap();
    assert_eq!(state.regions.len(), 1);
    assert_eq!(state.cells.len(), 1);
    assert_eq!(state.nodes.len(), 1);
    assert_eq!(state.regions["eu-west-1"].state, LifecycleState::Active);
    assert_eq!(
        state.cells["eu-west-1/cell-a"].state,
        LifecycleState::Active
    );
    assert_eq!(state.nodes["node-a"].state, LifecycleState::Active);
}

#[tokio::test]
async fn canonical_topology_activation_is_portable_and_cannot_be_removed() {
    let source_temp = tempdir().unwrap();
    let source_storage = Storage::new_at(source_temp.path()).await.unwrap();
    let source_store = CoreStore::new(source_storage.clone()).await.unwrap();
    let receipt_signing_public_keys = (0..3)
        .map(|_| test_receipt_signing_public_key())
        .collect::<Vec<_>>();
    let cells = (0..3)
        .map(|index| RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: format!("cell-{index}"),
            placement_weight: 100,
            failure_domain: format!("rack-{index}"),
        })
        .collect::<Vec<_>>();
    let nodes = receipt_signing_public_keys
        .iter()
        .enumerate()
        .map(
            |(index, receipt_signing_public_key)| RegisterNodeDescriptor {
                mesh_id: "mesh-a".to_string(),
                node_id: format!("node-{index}"),
                region: "eu-west-1".to_string(),
                cell_id: format!("cell-{index}"),
                receipt_signing_public_key: receipt_signing_public_key.clone(),
                public_api_addr: format!("http://127.0.0.1:{}", 50_051 + index),
                capabilities: vec![NodeCapability::Metadata, NodeCapability::Object],
                capacity_json: "{}".to_string(),
            },
        )
        .collect::<Vec<_>>();
    let bootstrap = BootstrapMeshLifecycleProjection {
        regions: vec![CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("cell-0".to_string()),
        }],
        cells,
        nodes,
    };
    let state =
        install_bootstrap_lifecycle_projection(&source_storage, &source_store, bootstrap.clone())
            .unwrap();
    let activation = state
        .canonical_topology_activation
        .clone()
        .expect("three metadata peers activate canonical mode");
    assert_eq!(activation.metadata_node_ids.len(), 3);
    assert!(
        activation
            .metadata_node_ids
            .iter()
            .all(|node| !is_synthetic_control_node_id(node))
    );
    let head = state
        .topology_head
        .as_ref()
        .expect("canonical bootstrap writes a topology head");
    assert_eq!(activation.mesh_id, "mesh-a");
    assert_eq!(activation.pre_activation_topology_head_generation, 0);
    assert_eq!(
        activation.pre_activation_topology_head_hash,
        topology_activation::empty_topology_hash().unwrap()
    );
    assert_eq!(activation.quorum_profile, CANONICAL_METADATA_QUORUM_PROFILE);
    assert_eq!(activation.topology_hash, head.topology_hash);
    assert!(activation.payload_hash.starts_with("sha256:"));

    let replayed =
        install_bootstrap_lifecycle_projection(&source_storage, &source_store, bootstrap.clone())
            .unwrap();
    assert_eq!(replayed, state);

    let mut mismatch = bootstrap;
    mismatch.nodes[2].public_api_addr = "http://127.0.0.1:59999".to_string();
    let error = install_bootstrap_lifecycle_projection(&source_storage, &source_store, mismatch)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("already installed canonical topology")
    );
    assert_eq!(
        read_lifecycle_state_projection_with_core_store(&source_store).unwrap(),
        state
    );

    let rows = source_store
        .export_portable_coremeta_bootstrap_rows(4096)
        .unwrap();
    let target_temp = tempdir().unwrap();
    let target_storage = Storage::new_at(target_temp.path()).await.unwrap();
    let target_store = CoreStore::new(target_storage).await.unwrap();
    target_store
        .install_portable_coremeta_bootstrap_rows(&rows)
        .unwrap();
    assert_eq!(
        canonical_topology_activation_with_core_store(&target_store)
            .unwrap()
            .as_ref(),
        Some(&activation)
    );

    let mut removal = state;
    removal.canonical_topology_activation = None;
    let error = write_state(&source_storage, &removal).await.unwrap_err();
    assert!(error.to_string().contains("cannot be replaced or removed"));
}

async fn create_test_region(storage: &Storage) -> RegionDescriptor {
    create_region(
        storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: None,
        },
    )
    .await
    .unwrap()
}

async fn register_test_cell(storage: &Storage) -> CellDescriptor {
    register_cell(
        storage,
        RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        },
    )
    .await
    .unwrap()
}

async fn register_test_node(storage: &Storage) -> NodeDescriptor {
    register_node(
        storage,
        RegisterNodeDescriptor {
            mesh_id: "mesh-a".to_string(),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            receipt_signing_public_key: test_receipt_signing_public_key(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
            capacity_json: "{}".to_string(),
        },
    )
    .await
    .unwrap()
}

async fn create_active_placement_model(
    storage: &Storage,
) -> (RegionDescriptor, CellDescriptor, NodeDescriptor) {
    let region = create_region(
        storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("cell-a".to_string()),
        },
    )
    .await
    .unwrap();
    let cell = register_cell(
        storage,
        RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        },
    )
    .await
    .unwrap();
    let cell = transition_cell(
        storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let region = transition_region(
        storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let node = register_node(
        storage,
        RegisterNodeDescriptor {
            mesh_id: "mesh-a".to_string(),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            receipt_signing_public_key: test_receipt_signing_public_key(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
            capacity_json: "{}".to_string(),
        },
    )
    .await
    .unwrap();
    let node = transition_node(
        storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    (region, cell, node)
}

fn checkpoint_with_stream(
    stream_family: &str,
    partition: &str,
    sequence: u64,
    digest: ControlRecordDigest,
) -> ActivationCheckpoint {
    ActivationCheckpoint {
        schema: ACTIVATION_CHECKPOINT_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        required_streams: vec![ActivationCheckpointStream {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            sequence: ControlStreamSequence::new(sequence).unwrap(),
            digest,
        }],
    }
}

fn digest_for(bytes: &[u8]) -> ControlRecordDigest {
    ControlRecordDigest::blake3(bytes)
}

async fn append_control_record(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    sequence: u64,
    digest: ControlRecordDigest,
) {
    let cursor =
        crate::mesh_control_stream::control_stream_append_cursor(storage, stream_family, partition)
            .await
            .unwrap();
    assert_eq!(cursor.sequence.get(), sequence);
    let header_proto = crate::mesh_control_stream::encode_control_mutation_header(
        crate::mesh_control_stream::ControlMutationHeaderInput {
            schema: "anvil.mesh.control_mutation.v1",
            mesh_id: "mesh-a",
            stream_family,
            partition,
            sequence: ControlStreamSequence::new(sequence).unwrap(),
            record_key: "tenant_acme/releases",
            operation: "upsert",
            expected_generation: Some(1),
            new_generation: 2,
            writer_node_id: "node-a",
            writer_fence: 1,
            idempotency_key: Some("idem-a"),
            record_digest: &digest,
            created_at: "2026-07-02T00:00:00Z",
            byte_offset: cursor.byte_offset,
        },
    );
    crate::mesh_control_stream::append_control_stream_frame(
        storage,
        stream_family,
        partition,
        &crate::mesh_control_stream::ControlStreamFrame::new(
            header_proto,
            br#"{"ok":true}"#.to_vec(),
        ),
        None,
    )
    .await
    .unwrap();
}

async fn write_control_checkpoint_record(
    storage: &Storage,
    region: &str,
    stream_family: &str,
    partition: &str,
    sequence: u64,
    digest: ControlRecordDigest,
) {
    crate::mesh_control_stream::write_control_checkpoint(
        storage,
        &crate::mesh_control_stream::ControlCheckpointRecord::new(
            "mesh-a",
            region,
            stream_family,
            partition,
            ControlStreamSequence::new(sequence).unwrap(),
            digest,
            "2026-07-02T00:00:00Z",
        ),
    )
    .await
    .unwrap();
}

async fn append_lifecycle_descriptor<T: record_proto::LifecycleControlPayload>(
    storage: &Storage,
    stream_family: &str,
    record_key: &str,
    sequence: u64,
    descriptor: &T,
) {
    let partition = lifecycle_control_partition(stream_family, record_key);
    let cursor = crate::mesh_control_stream::control_stream_append_cursor(
        storage,
        stream_family,
        &partition,
    )
    .await
    .unwrap();
    assert_eq!(cursor.sequence.get(), sequence);
    let payload_proto =
        record_proto::encode_lifecycle_control_payload(descriptor, stream_family).unwrap();
    let digest = ControlRecordDigest::blake3(&payload_proto);
    let header_proto = crate::mesh_control_stream::encode_control_mutation_header(
        crate::mesh_control_stream::ControlMutationHeaderInput {
            schema: "anvil.mesh.control_mutation.v1",
            mesh_id: "mesh-a",
            stream_family,
            partition: &partition,
            sequence: ControlStreamSequence::new(sequence).unwrap(),
            record_key,
            operation: "upsert",
            expected_generation: Some(sequence.saturating_sub(1)),
            new_generation: sequence,
            writer_node_id: "node-a",
            writer_fence: 1,
            idempotency_key: Some("idem-a"),
            record_digest: &digest,
            created_at: "2026-07-02T00:00:00Z",
            byte_offset: cursor.byte_offset,
        },
    );
    crate::mesh_control_stream::append_control_stream_frame(
        storage,
        stream_family,
        &partition,
        &crate::mesh_control_stream::ControlStreamFrame::new(header_proto, payload_proto),
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn host_aliases_are_generation_checked_and_region_bound() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let routing_config = RoutingConfig::new("anvil-storage.com").unwrap();

    let region = create_region(
        &storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.com".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.com".to_string(),
            placement_weight: 100,
            default_cell: None,
        },
    )
    .await
    .unwrap();
    transition_region(
        &storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();

    let alias = create_host_alias(
        &storage,
        &routing_config,
        CreateHostAliasDescriptor {
            hostname: "CDN.Example.Com.".to_string(),
            tenant_id: "tenant-acme".to_string(),
            bucket_name: "releases".to_string(),
            region: "eu-west-1".to_string(),
            prefix: "public/".to_string(),
        },
    )
    .await
    .unwrap();

    assert_eq!(alias.hostname, "cdn.example.com");
    assert_eq!(alias.state, HostAliasState::PendingVerification);
    let stale = transition_host_alias(&storage, "cdn.example.com", 99, HostAliasState::Active)
        .await
        .unwrap_err();
    assert!(matches!(stale, LifecycleError::GenerationConflict { .. }));

    let active = transition_host_alias(
        &storage,
        "cdn.example.com",
        alias.generation,
        HostAliasState::Active,
    )
    .await
    .unwrap();
    assert_eq!(active.state, HostAliasState::Active);
    assert_eq!(active.generation, 2);

    let aliases = list_host_aliases(&storage, Some("eu-west-1"))
        .await
        .unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].hostname, "cdn.example.com");
}
